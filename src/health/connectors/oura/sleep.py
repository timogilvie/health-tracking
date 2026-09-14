"""Oura sleep, readiness, and heart-rate ingestion."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Protocol
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from oura_ring import OuraClient
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from health.connectors import RawPage
from health.connectors.oura.oauth import OuraOAuth
from health.ingestion import NormalizedRecord, RawRef, RawStore, RetryableIngestionError


class OuraPayloadError(ValueError):
    """A stored Oura page does not match the collection contract."""


class OuraAPIError(RuntimeError):
    """A sanitized non-retryable Oura API failure."""

    def __init__(self, *, http_status: int | None = None) -> None:
        self.http_status = http_status
        if http_status in {401, 403}:
            category = "authentication_failed"
        elif http_status is not None and http_status < 500:
            category = "invalid_request"
        else:
            category = "provider_unavailable"
        super().__init__(f"Oura API request failed ({category})")


class OuraCollectionEnvelope(BaseModel):
    model_config = ConfigDict(extra="allow")

    data: list[dict[str, Any]] = Field(default_factory=list)
    next_token: str | None = None


class OuraDataClient(Protocol):
    session: requests.Session

    def __enter__(self) -> OuraDataClient: ...

    def __exit__(self, *_: object) -> None: ...

    def get_sleep_periods(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        document_id: str | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]: ...

    def get_daily_sleep(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        document_id: str | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]: ...

    def get_daily_readiness(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        document_id: str | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]: ...

    def get_heart_rate(
        self,
        start_datetime: str | None = None,
        end_datetime: str | None = None,
        latest: bool | None = None,
    ) -> list[dict[str, Any]]: ...

    def get_daily_activity(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        document_id: str | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]: ...

    def get_workouts(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        document_id: str | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]: ...

    def get_sessions(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        document_id: str | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]: ...


def parse_collection_envelope(content: bytes) -> OuraCollectionEnvelope:
    try:
        return OuraCollectionEnvelope.model_validate_json(content)
    except ValidationError as exc:
        raise OuraPayloadError("Oura collection schema validation failed") from exc


def _aware_timestamp(value: Any, *, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise OuraPayloadError(f"Oura {field_name} is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OuraPayloadError(f"Oura {field_name} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OuraPayloadError(f"Oura {field_name} must include a timezone")
    return parsed


def _day_timestamp(record: dict[str, Any], *, timezone: ZoneInfo) -> tuple[datetime, date]:
    day_value = record.get("day")
    try:
        local_date = date.fromisoformat(str(day_value))
    except ValueError as exc:
        raise OuraPayloadError("Oura day is invalid") from exc
    timestamp = record.get("timestamp")
    if timestamp is not None:
        return _aware_timestamp(timestamp, field_name="timestamp"), local_date
    return datetime.combine(local_date, time.min, tzinfo=timezone), local_date


def _source_record_id(record: dict[str, Any], *, kind: str) -> tuple[str, list[str]]:
    identifier = record.get("id")
    if isinstance(identifier, str) and identifier:
        return identifier, []
    document = json.dumps(record, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(document.encode()).hexdigest()[:24]
    return f"missing-id:{kind}:{digest}", ["missing_source_record_id"]


def _optional_number(value: Any, *, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise OuraPayloadError(f"Oura {field_name} is not numeric")
    number = float(value)
    if not math.isfinite(number):
        raise OuraPayloadError(f"Oura {field_name} is not finite")
    return number


def _optional_positive_number(
    value: Any,
    *,
    field_name: str,
    quality_reasons: list[str],
) -> float | None:
    """Treat provider zero sentinels as missing for positive-only vital signs."""

    number = _optional_number(value, field_name=field_name)
    if number is not None and number <= 0:
        quality_reasons.append(f"non_positive_{field_name}")
        return None
    return number


def _optional_duration(value: Any, *, field_name: str) -> int | None:
    number = _optional_number(value, field_name=field_name)
    if number is None:
        return None
    if number < 0 or not number.is_integer():
        raise OuraPayloadError(f"Oura {field_name} is not a non-negative duration")
    return int(number)


def _observation(
    *,
    metric: str,
    value: float,
    unit: str,
    observed_at: datetime,
    local_date: date,
    timezone_name: str,
    source_record_id: str,
    original_metric: str,
    transform_version: str,
    metadata: dict[str, Any],
    plausible_min: float,
    plausible_max: float,
    original_value: float | None = None,
    original_unit: str | None = None,
) -> NormalizedRecord:
    quality_reasons = list(metadata.get("quality_reasons", []))
    if not plausible_min <= value <= plausible_max:
        quality_reasons.append("implausible_value")
    return NormalizedRecord(
        record_type="observation",
        identity=f"{source_record_id}:{metric}",
        values={
            "metric": metric,
            "observed_at": observed_at,
            "value": value,
            "unit": unit,
            "source": "oura",
            "source_record_id": source_record_id,
            "device": None,
            "original_metric": original_metric,
            "original_value": value if original_value is None else original_value,
            "original_unit": unit if original_unit is None else original_unit,
            "quality": "suspect" if quality_reasons else "valid",
            "timezone": timezone_name,
            "local_date": local_date,
            "transform_version": transform_version,
            "metadata": {**metadata, "quality_reasons": quality_reasons},
        },
    )


class OuraSleepConnector:
    """Fetch Oura recovery pages and normalize sleep sessions and scalar metrics."""

    name = "oura"
    transform_version = "oura-sleep-v1"
    heart_rate_max_window = timedelta(days=30)

    def __init__(
        self,
        *,
        oauth: OuraOAuth,
        timezone_name: str,
        client_factory: Callable[[str], OuraDataClient] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        try:
            self.timezone = ZoneInfo(timezone_name)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError(f"invalid project timezone: {timezone_name}") from exc
        self.oauth = oauth
        self.timezone_name = timezone_name
        self.client_factory = client_factory or (lambda token: OuraClient(access_token=token))
        self.now = now or (lambda: datetime.now(UTC))

    def authenticate(self) -> None:
        self.oauth.authenticate()

    def _capture(self, response: requests.Response, *_args: Any, **_kwargs: Any) -> None:
        request = response.request
        parsed = urlparse(request.url)
        retrieved_at = self.now()
        if retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
            raise ValueError("Oura retrieval time must be timezone-aware")
        self._captured.append(
            RawPage(
                source=self.name,
                endpoint=parsed.path.lstrip("/"),
                retrieved_at=retrieved_at,
                content=response.content,
                content_type=response.headers.get("Content-Type", "application/json"),
                request_metadata={
                    "method": request.method,
                    "query": parse_qs(parsed.query),
                },
                http_status=response.status_code,
                response_headers=dict(response.headers),
            )
        )

    @staticmethod
    def _request_error(error: requests.RequestException) -> Exception:
        status = error.response.status_code if error.response is not None else None
        if status is None or status == 429 or status >= 500:
            return RetryableIngestionError("Oura API temporarily unavailable")
        return OuraAPIError(http_status=status)

    def _resource_calls(
        self,
        client: OuraDataClient,
        *,
        local_start: str,
        local_end: str,
        start: datetime,
        end: datetime,
    ) -> tuple[Callable[[], object], ...]:
        calls: list[Callable[[], object]] = [
            lambda: client.get_sleep_periods(local_start, local_end),
            lambda: client.get_daily_sleep(local_start, local_end),
            lambda: client.get_daily_readiness(local_start, local_end),
        ]
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + self.heart_rate_max_window, end)
            calls.append(
                lambda chunk_start=chunk_start, chunk_end=chunk_end: client.get_heart_rate(
                    chunk_start.isoformat(), chunk_end.isoformat()
                )
            )
            chunk_start = chunk_end
        if start == end:
            calls.append(lambda: client.get_heart_rate(start.isoformat(), end.isoformat()))
        return tuple(calls)

    def fetch(self, start: datetime, end: datetime) -> Iterable[RawPage]:
        if start.tzinfo is None or start.utcoffset() is None:
            raise ValueError("Oura sync start must be timezone-aware")
        if end.tzinfo is None or end.utcoffset() is None:
            raise ValueError("Oura sync end must be timezone-aware")
        if start > end:
            raise ValueError("Oura sync start must not be after end")
        local_start = start.astimezone(self.timezone).date().isoformat()
        local_end = end.astimezone(self.timezone).date().isoformat()
        self._captured: list[RawPage] = []
        with self.client_factory(self.oauth.access_token()) as client:
            client.session.hooks.setdefault("response", []).append(self._capture)
            calls = self._resource_calls(
                client,
                local_start=local_start,
                local_end=local_end,
                start=start,
                end=end,
            )
            for call in calls:
                first_page = len(self._captured)
                try:
                    call()
                except requests.RequestException as exc:
                    yield from self._captured[first_page:]
                    raise self._request_error(exc) from exc
                yield from self._captured[first_page:]

    def normalize(self, raw_ref: RawRef, raw_store: RawStore) -> Iterable[NormalizedRecord]:
        manifest = raw_store.manifest(raw_ref)
        http_status = manifest.get("http_status")
        if not isinstance(http_status, int) or not 200 <= http_status < 300:
            return ()
        envelope = parse_collection_envelope(raw_store.read(raw_ref))
        endpoint = str(manifest.get("endpoint", ""))
        if endpoint.endswith("/sleep"):
            return self._sleep_sessions(envelope)
        if endpoint.endswith("/daily_sleep"):
            return self._daily_sleep(envelope)
        if endpoint.endswith("/daily_readiness"):
            return self._daily_readiness(envelope)
        if endpoint.endswith("/heartrate"):
            return self._heart_rate(envelope)
        raise OuraPayloadError(f"unsupported Oura endpoint: {endpoint}")

    def _metadata(
        self,
        *,
        endpoint: str,
        record: dict[str, Any],
        envelope: OuraCollectionEnvelope,
        quality_reasons: list[str],
    ) -> dict[str, Any]:
        return {
            "oura": {
                "endpoint": endpoint,
                "record": record,
                "response": {"next_token": envelope.next_token},
            },
            "quality_reasons": quality_reasons,
        }

    def _sleep_sessions(self, envelope: OuraCollectionEnvelope) -> list[NormalizedRecord]:
        records: list[NormalizedRecord] = []
        for record in envelope.data:
            source_record_id, quality_reasons = _source_record_id(record, kind="sleep")
            started_at = _aware_timestamp(record.get("bedtime_start"), field_name="bedtime_start")
            ended_at = _aware_timestamp(record.get("bedtime_end"), field_name="bedtime_end")
            if ended_at < started_at:
                raise OuraPayloadError("Oura sleep ends before it starts")
            time_in_bed = _optional_duration(record.get("time_in_bed"), field_name="time_in_bed")
            awake = _optional_duration(record.get("awake_time"), field_name="awake_time")
            total_sleep = _optional_duration(
                record.get("total_sleep_duration"),
                field_name="total_sleep_duration",
            )
            if total_sleep is None and time_in_bed is not None and awake is not None:
                total_sleep = max(time_in_bed - awake, 0)
                quality_reasons.append("derived_total_sleep")
            efficiency = _optional_number(record.get("efficiency"), field_name="efficiency")
            if efficiency is None and time_in_bed and total_sleep is not None:
                efficiency = total_sleep / time_in_bed * 100
                quality_reasons.append("derived_efficiency")
            records.append(
                NormalizedRecord(
                    record_type="sleep_session",
                    identity=source_record_id,
                    values={
                        "sleep_date": ended_at.date(),
                        "started_at": started_at,
                        "ended_at": ended_at,
                        "time_in_bed_seconds": time_in_bed,
                        "total_sleep_seconds": total_sleep,
                        "awake_seconds": awake,
                        "light_seconds": _optional_duration(
                            record.get("light_sleep_duration"),
                            field_name="light_sleep_duration",
                        ),
                        "deep_seconds": _optional_duration(
                            record.get("deep_sleep_duration"),
                            field_name="deep_sleep_duration",
                        ),
                        "rem_seconds": _optional_duration(
                            record.get("rem_sleep_duration"),
                            field_name="rem_sleep_duration",
                        ),
                        "latency_seconds": _optional_duration(
                            record.get("latency"),
                            field_name="latency",
                        ),
                        "efficiency_pct": efficiency,
                        "resting_hr_bpm": _optional_positive_number(
                            record.get("average_heart_rate"),
                            field_name="average_heart_rate",
                            quality_reasons=quality_reasons,
                        ),
                        "lowest_hr_bpm": _optional_positive_number(
                            record.get("lowest_heart_rate"),
                            field_name="lowest_heart_rate",
                            quality_reasons=quality_reasons,
                        ),
                        "average_hrv_rmssd_ms": _optional_number(
                            record.get("average_hrv"),
                            field_name="average_hrv",
                        ),
                        "respiratory_rate": _optional_positive_number(
                            record.get("average_breath"),
                            field_name="average_breath",
                            quality_reasons=quality_reasons,
                        ),
                        "sleep_score": None,
                        "source": self.name,
                        "source_record_id": source_record_id,
                        "device": None,
                        "transform_version": self.transform_version,
                        "metadata": self._metadata(
                            endpoint="sleep",
                            record=record,
                            envelope=envelope,
                            quality_reasons=quality_reasons,
                        ),
                    },
                )
            )
        return records

    def _daily_sleep(self, envelope: OuraCollectionEnvelope) -> list[NormalizedRecord]:
        records: list[NormalizedRecord] = []
        for record in envelope.data:
            score = _optional_number(record.get("score"), field_name="score")
            if score is None:
                continue
            source_record_id, quality_reasons = _source_record_id(record, kind="daily-sleep")
            observed_at, local_date = _day_timestamp(record, timezone=self.timezone)
            metadata = self._metadata(
                endpoint="daily_sleep",
                record=record,
                envelope=envelope,
                quality_reasons=quality_reasons,
            )
            records.append(
                _observation(
                    metric="sleep_score",
                    value=score,
                    unit="score",
                    observed_at=observed_at,
                    local_date=local_date,
                    timezone_name=self.timezone_name,
                    source_record_id=source_record_id,
                    original_metric="score",
                    transform_version=self.transform_version,
                    metadata=metadata,
                    plausible_min=0,
                    plausible_max=100,
                )
            )
        return records

    def _daily_readiness(self, envelope: OuraCollectionEnvelope) -> list[NormalizedRecord]:
        definitions = {
            "score": ("readiness_score", "score", 0.0, 100.0),
            "temperature_deviation": ("temperature_deviation_c", "C", -10.0, 10.0),
            "temperature_trend_deviation": (
                "temperature_trend_deviation_c",
                "C",
                -10.0,
                10.0,
            ),
        }
        records: list[NormalizedRecord] = []
        for record in envelope.data:
            source_record_id, identity_reasons = _source_record_id(record, kind="daily-readiness")
            observed_at, local_date = _day_timestamp(record, timezone=self.timezone)
            for original_metric, definition in definitions.items():
                metric, unit, plausible_min, plausible_max = definition
                value = _optional_number(record.get(original_metric), field_name=original_metric)
                if value is None:
                    continue
                metadata = self._metadata(
                    endpoint="daily_readiness",
                    record=record,
                    envelope=envelope,
                    quality_reasons=identity_reasons,
                )
                records.append(
                    _observation(
                        metric=metric,
                        value=value,
                        unit=unit,
                        observed_at=observed_at,
                        local_date=local_date,
                        timezone_name=self.timezone_name,
                        source_record_id=source_record_id,
                        original_metric=original_metric,
                        transform_version=self.transform_version,
                        metadata=metadata,
                        plausible_min=plausible_min,
                        plausible_max=plausible_max,
                    )
                )
        return records

    def _heart_rate(self, envelope: OuraCollectionEnvelope) -> list[NormalizedRecord]:
        records: list[NormalizedRecord] = []
        for record in envelope.data:
            observed_at = _aware_timestamp(record.get("timestamp"), field_name="timestamp")
            bpm = _optional_number(record.get("bpm"), field_name="bpm")
            if bpm is None:
                continue
            identifier = record.get("id")
            if isinstance(identifier, str) and identifier:
                source_record_id = identifier
            else:
                source = record.get("source")
                source_record_id = f"heart-rate:{observed_at.astimezone(UTC).isoformat()}:{source}"
            records.append(
                _observation(
                    metric="heart_rate_bpm",
                    value=bpm,
                    unit="bpm",
                    observed_at=observed_at,
                    local_date=observed_at.astimezone(self.timezone).date(),
                    timezone_name=self.timezone_name,
                    source_record_id=source_record_id,
                    original_metric="bpm",
                    transform_version=self.transform_version,
                    metadata=self._metadata(
                        endpoint="heartrate",
                        record=record,
                        envelope=envelope,
                        quality_reasons=[],
                    ),
                    plausible_min=25,
                    plausible_max=250,
                )
            )
        return records


WORKOUT_TYPE_MAP = {
    "strength_training": "resistance",
    "weightlifting": "resistance",
    "walking": "walking",
    "hiking": "walking",
    "running": "running",
    "jogging": "running",
    "cycling": "cycling",
    "indoor_cycling": "cycling",
    "rowing": "rowing",
    "swimming": "swimming",
    "elliptical": "elliptical",
    "yoga": "mobility",
    "stretching": "mobility",
    "mobility": "mobility",
    "basketball": "sports",
    "football": "sports",
    "soccer": "sports",
    "tennis": "sports",
    "volleyball": "sports",
}


class OuraConnector(OuraSleepConnector):
    """Complete Oura MVP connector with one shared watermark and replay boundary."""

    transform_version = "oura-v1"

    def _resource_calls(
        self,
        client: OuraDataClient,
        *,
        local_start: str,
        local_end: str,
        start: datetime,
        end: datetime,
    ) -> tuple[Callable[[], object], ...]:
        recovery = super()._resource_calls(
            client,
            local_start=local_start,
            local_end=local_end,
            start=start,
            end=end,
        )
        return recovery[:3] + (
            lambda: client.get_daily_activity(local_start, local_end),
            lambda: client.get_workouts(local_start, local_end),
            lambda: client.get_sessions(local_start, local_end),
        ) + recovery[3:]

    def normalize(self, raw_ref: RawRef, raw_store: RawStore) -> Iterable[NormalizedRecord]:
        manifest = raw_store.manifest(raw_ref)
        http_status = manifest.get("http_status")
        if not isinstance(http_status, int) or not 200 <= http_status < 300:
            return ()
        endpoint = str(manifest.get("endpoint", ""))
        if endpoint.endswith("/daily_activity"):
            return self._daily_activity(parse_collection_envelope(raw_store.read(raw_ref)))
        if endpoint.endswith("/workout"):
            return self._workouts(parse_collection_envelope(raw_store.read(raw_ref)))
        if endpoint.endswith("/session"):
            return self._sessions(parse_collection_envelope(raw_store.read(raw_ref)))
        return super().normalize(raw_ref, raw_store)

    def _daily_activity(self, envelope: OuraCollectionEnvelope) -> list[NormalizedRecord]:
        definitions = {
            "score": ("activity_score", "score", 1.0, 0.0, 100.0),
            "steps": ("steps", "count", 1.0, 0.0, 200_000.0),
            "active_calories": (
                "active_energy_kcal",
                "kcal",
                1.0,
                0.0,
                20_000.0,
            ),
            "total_calories": ("total_energy_kcal", "kcal", 1.0, 0.0, 30_000.0),
            "equivalent_walking_distance": (
                "equivalent_walking_distance_m",
                "m",
                1.0,
                0.0,
                500_000.0,
            ),
            "high_activity_time": (
                "high_activity_minutes",
                "min",
                1 / 60,
                0.0,
                1_440.0,
            ),
            "medium_activity_time": (
                "medium_activity_minutes",
                "min",
                1 / 60,
                0.0,
                1_440.0,
            ),
            "low_activity_time": (
                "low_activity_minutes",
                "min",
                1 / 60,
                0.0,
                1_440.0,
            ),
            "sedentary_time": ("sedentary_minutes", "min", 1 / 60, 0.0, 1_440.0),
            "non_wear_time": ("non_wear_minutes", "min", 1 / 60, 0.0, 1_440.0),
        }
        normalized: list[NormalizedRecord] = []
        for record in envelope.data:
            source_record_id, identity_reasons = _source_record_id(
                record,
                kind="daily-activity",
            )
            observed_at, local_date = _day_timestamp(record, timezone=self.timezone)
            non_wear = _optional_duration(record.get("non_wear_time"), field_name="non_wear_time")
            for original_metric, definition in definitions.items():
                metric, unit, multiplier, plausible_min, plausible_max = definition
                original_value = _optional_number(
                    record.get(original_metric),
                    field_name=original_metric,
                )
                if original_value is None:
                    continue
                value = original_value * multiplier
                metadata = self._metadata(
                    endpoint="daily_activity",
                    record=record,
                    envelope=envelope,
                    quality_reasons=identity_reasons,
                )
                metadata["coverage"] = {
                    "non_wear_seconds": non_wear,
                    "missing_values_are_unknown": True,
                }
                normalized.append(
                    _observation(
                        metric=metric,
                        value=value,
                        unit=unit,
                        observed_at=observed_at,
                        local_date=local_date,
                        timezone_name=self.timezone_name,
                        source_record_id=source_record_id,
                        original_metric=original_metric,
                        original_value=original_value,
                        original_unit="s" if original_metric.endswith("_time") else unit,
                        transform_version=self.transform_version,
                        metadata=metadata,
                        plausible_min=plausible_min,
                        plausible_max=plausible_max,
                    )
                )
        return normalized

    def _workouts(self, envelope: OuraCollectionEnvelope) -> list[NormalizedRecord]:
        normalized: list[NormalizedRecord] = []
        for record in envelope.data:
            source_record_id, quality_reasons = _source_record_id(record, kind="workout")
            started_at = _aware_timestamp(
                record.get("start_datetime"),
                field_name="start_datetime",
            )
            ended_at = _aware_timestamp(record.get("end_datetime"), field_name="end_datetime")
            if ended_at < started_at:
                raise OuraPayloadError("Oura workout ends before it starts")
            activity = str(record.get("activity") or "unknown").strip().lower()
            workout_type = WORKOUT_TYPE_MAP.get(activity, "other")
            normalized.append(
                NormalizedRecord(
                    record_type="workout",
                    identity=source_record_id,
                    values={
                        "started_at": started_at,
                        "ended_at": ended_at,
                        "local_date": started_at.astimezone(self.timezone).date(),
                        "workout_type": workout_type,
                        "duration_seconds": int((ended_at - started_at).total_seconds()),
                        "intensity": record.get("intensity"),
                        "rpe": None,
                        "distance_m": _optional_number(
                            record.get("distance"),
                            field_name="distance",
                        ),
                        "energy_kcal": _optional_number(
                            record.get("calories"),
                            field_name="calories",
                        ),
                        "average_hr_bpm": None,
                        "max_hr_bpm": None,
                        "source": self.name,
                        "source_record_id": source_record_id,
                        "device": None,
                        "notes": record.get("label"),
                        "transform_version": self.transform_version,
                        "metadata": self._metadata(
                            endpoint="workout",
                            record=record,
                            envelope=envelope,
                            quality_reasons=quality_reasons,
                        ),
                    },
                )
            )
        return normalized

    def _sessions(self, envelope: OuraCollectionEnvelope) -> list[NormalizedRecord]:
        normalized: list[NormalizedRecord] = []
        for record in envelope.data:
            source_record_id, quality_reasons = _source_record_id(record, kind="session")
            started_at = _aware_timestamp(
                record.get("start_datetime"),
                field_name="start_datetime",
            )
            ended_at = _aware_timestamp(record.get("end_datetime"), field_name="end_datetime")
            if ended_at < started_at:
                raise OuraPayloadError("Oura session ends before it starts")
            normalized.append(
                NormalizedRecord(
                    record_type="event",
                    identity=source_record_id,
                    values={
                        "event_type": "oura_session",
                        "started_at": started_at,
                        "ended_at": ended_at,
                        "local_date": started_at.astimezone(self.timezone).date(),
                        "value": None,
                        "unit": None,
                        "notes": None,
                        "source": self.name,
                        "source_record_id": source_record_id,
                        "transform_version": self.transform_version,
                        "metadata": self._metadata(
                            endpoint="session",
                            record=record,
                            envelope=envelope,
                            quality_reasons=quality_reasons,
                        ),
                    },
                )
            )
        return normalized
