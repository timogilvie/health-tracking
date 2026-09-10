"""Withings measurement ingestion behind the project connector contract.

The request and schema seams are adapted from Open Wearables at commit
802862fa1ac08f165a897cb9adc4f4b312b08e5c. Copyright (c) 2025 Momentum,
used under the MIT License. See NOTICE.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from health.connectors.base import RawPage
from health.connectors.withings.oauth import WithingsOAuth
from health.connectors.withings.rpc import WithingsPayloadError, WithingsRPCClient
from health.ingestion import NormalizedRecord, RawRef, RawStore

MEASURE_SERVICE_PATH = "/measure"
MEASURE_ACTION = "getmeas"
MEASURE_LIST_KEY = "measuregrps"

# Narrow HOK-2978 scope: weight, body composition, blood pressure, and pulse.
# The numeric IDs match Withings' getmeas contract and the reviewed upstream coverage map.
MEASURE_TYPE_CODES = (1, 5, 6, 8, 9, 10, 11, 76, 77, 88)


class MeasurementDefinition(BaseModel):
    metric: str
    unit: str
    plausible_min: float
    plausible_max: float


BODY_COMPOSITION_MEASURES: dict[int, MeasurementDefinition] = {
    1: MeasurementDefinition(
        metric="weight_kg", unit="kg", plausible_min=20, plausible_max=500
    ),
    5: MeasurementDefinition(
        metric="lean_mass_kg", unit="kg", plausible_min=5, plausible_max=300
    ),
    6: MeasurementDefinition(
        metric="body_fat_pct", unit="%", plausible_min=1, plausible_max=75
    ),
    8: MeasurementDefinition(
        metric="body_fat_mass_kg", unit="kg", plausible_min=0, plausible_max=300
    ),
    76: MeasurementDefinition(
        metric="skeletal_muscle_mass_kg", unit="kg", plausible_min=0, plausible_max=250
    ),
    77: MeasurementDefinition(
        metric="body_water_mass_kg", unit="kg", plausible_min=0, plausible_max=250
    ),
    88: MeasurementDefinition(
        metric="bone_mass_kg", unit="kg", plausible_min=0, plausible_max=30
    ),
}


class WithingsMeasure(BaseModel):
    """Preserve a vendor measure and its base-10 exponent without scaling it."""

    model_config = ConfigDict(extra="allow")

    value: int
    type: int = Field(ge=0)
    unit: int
    position: int | None = None


class WithingsMeasureGroup(BaseModel):
    """A timestamped vendor group; grpid remains the future canonical identity."""

    model_config = ConfigDict(extra="allow")

    date: int
    timezone: str | None = None
    measures: list[WithingsMeasure] = Field(default_factory=list)
    grpid: int | None = None
    attrib: int | None = None
    category: int | None = None
    deviceid: str | None = None
    model: str | None = None


class WithingsMeasurementBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    measuregrps: list[WithingsMeasureGroup] = Field(default_factory=list)
    more: int = Field(default=0, ge=0, le=1)
    offset: int = Field(default=0, ge=0)
    timezone: str | None = None


class WithingsMeasurementEnvelope(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: int
    body: WithingsMeasurementBody | None = None

    @model_validator(mode="after")
    def successful_response_has_body(self) -> WithingsMeasurementEnvelope:
        if self.status == 0 and self.body is None:
            raise ValueError("successful envelope requires a body")
        return self


def parse_measurement_envelope(content: bytes) -> WithingsMeasurementEnvelope:
    """Validate a stored envelope without discarding vendor fields from raw storage."""

    try:
        return WithingsMeasurementEnvelope.model_validate_json(content)
    except ValidationError as exc:
        raise WithingsPayloadError(MEASURE_ACTION, "measurement schema validation failed") from exc


def _source_record_id(group: WithingsMeasureGroup) -> tuple[str, list[str]]:
    if group.grpid is not None:
        return str(group.grpid), []
    document = json.dumps(group.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(document.encode()).hexdigest()[:24]
    return f"missing-grpid:{group.date}:{digest}", ["missing_grpid"]


def _timestamp_context(
    group: WithingsMeasureGroup,
    response_timezone: str | None,
) -> tuple[datetime, str | None, date, list[str]]:
    try:
        observed_at = datetime.fromtimestamp(group.date, tz=UTC)
    except (OSError, OverflowError, ValueError) as exc:
        raise WithingsPayloadError(MEASURE_ACTION, "measurement timestamp is invalid") from exc
    timezone_name = group.timezone or response_timezone
    if timezone_name is None:
        return observed_at, None, observed_at.date(), ["missing_timezone"]
    try:
        local_date = observed_at.astimezone(ZoneInfo(timezone_name)).date()
    except (ValueError, ZoneInfoNotFoundError):
        return observed_at, None, observed_at.date(), ["invalid_timezone"]
    return observed_at, timezone_name, local_date, []


def _scale_measure(measure: WithingsMeasure) -> float:
    try:
        value = float(Decimal(measure.value).scaleb(measure.unit))
    except (InvalidOperation, OverflowError) as exc:
        raise WithingsPayloadError(MEASURE_ACTION, "measurement cannot be scaled") from exc
    if not math.isfinite(value):
        raise WithingsPayloadError(MEASURE_ACTION, "measurement is not finite")
    return value


class WithingsMeasurementConnector:
    """Fetch envelopes and normalize body measures; HOK-2980 adds blood pressure."""

    name = "withings"
    transform_version = "withings-measurements-v2"

    def __init__(
        self,
        *,
        oauth: WithingsOAuth,
        client: httpx.Client,
        now: Callable[[], datetime] | None = None,
        api_base_url: str = "https://wbsapi.withings.net",
        max_pages: int = 200,
    ) -> None:
        self.oauth = oauth
        self.rpc = WithingsRPCClient(
            client=client,
            access_token=oauth.access_token,
            now=now or (lambda: datetime.now(UTC)),
            api_base_url=api_base_url,
            max_pages=max_pages,
        )

    def authenticate(self) -> None:
        self.oauth.authenticate()

    def fetch(self, start: datetime, end: datetime) -> Iterable[RawPage]:
        if start.tzinfo is None or start.utcoffset() is None:
            raise ValueError("Withings sync start must be timezone-aware")
        if end.tzinfo is None or end.utcoffset() is None:
            raise ValueError("Withings sync end must be timezone-aware")
        if start > end:
            raise ValueError("Withings sync start must not be after end")
        return self.rpc.paginate(
            service_path=MEASURE_SERVICE_PATH,
            action=MEASURE_ACTION,
            params={
                "meastypes": ",".join(str(code) for code in MEASURE_TYPE_CODES),
                "category": 1,
                "startdate": int(start.timestamp()),
                "enddate": int(end.timestamp()),
            },
            list_key=MEASURE_LIST_KEY,
        )

    def normalize(
        self,
        raw_ref: RawRef,
        raw_store: RawStore,
    ) -> Iterable[NormalizedRecord]:
        manifest = raw_store.manifest(raw_ref)
        http_status = manifest.get("http_status")
        if not isinstance(http_status, int) or not 200 <= http_status < 300:
            return ()
        envelope = parse_measurement_envelope(raw_store.read(raw_ref))
        if envelope.status != 0 or envelope.body is None:
            return ()
        records: list[NormalizedRecord] = []
        response_context = envelope.body.model_dump(
            mode="json",
            exclude={"measuregrps"},
        )
        for group in envelope.body.measuregrps:
            source_record_id, identity_reasons = _source_record_id(group)
            observed_at, timezone_name, local_date, time_reasons = _timestamp_context(
                group,
                envelope.body.timezone,
            )
            for measure in group.measures:
                definition = BODY_COMPOSITION_MEASURES.get(measure.type)
                if definition is None:
                    continue
                value = _scale_measure(measure)
                quality_reasons = [*identity_reasons, *time_reasons]
                if not definition.plausible_min <= value <= definition.plausible_max:
                    quality_reasons.append("implausible_value")
                metadata = {
                    "withings": {
                        "grpid": group.grpid,
                        "group": group.model_dump(mode="json"),
                        "measure": measure.model_dump(mode="json"),
                        "response": response_context,
                    },
                    "quality_reasons": quality_reasons,
                }
                device = None
                if group.deviceid:
                    device = {
                        "manufacturer": "Withings",
                        "model": group.model,
                        "vendor_device_id": group.deviceid,
                    }
                records.append(
                    NormalizedRecord(
                        record_type="observation",
                        identity=f"{source_record_id}:{definition.metric}",
                        values={
                            "metric": definition.metric,
                            "observed_at": observed_at,
                            "value": value,
                            "unit": definition.unit,
                            "source": self.name,
                            "source_record_id": source_record_id,
                            "device": device,
                            "original_metric": f"withings_meastype_{measure.type}",
                            "original_value": float(measure.value),
                            "original_unit": f"10^{measure.unit} {definition.unit}",
                            "quality": "suspect" if quality_reasons else "valid",
                            "timezone": timezone_name,
                            "local_date": local_date,
                            "transform_version": self.transform_version,
                            "metadata": metadata,
                        },
                    )
                )
        return records
