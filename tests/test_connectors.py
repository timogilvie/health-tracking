from datetime import UTC, datetime

from health.connectors import RawPage


def test_raw_page_preserves_exact_bytes() -> None:
    content = b'{"weight": 82.1}'
    page = RawPage(
        source="withings",
        endpoint="measure/getmeas",
        retrieved_at=datetime.now(UTC),
        content=content,
    )

    assert page.content is content
    assert page.source == "withings"
