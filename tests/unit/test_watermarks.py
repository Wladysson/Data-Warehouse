from datetime import datetime, timedelta, timezone

from src.streaming.watermarks.timestamp_assigner import EventTimestampAssigner
from src.streaming.watermarks.watermark_strategy import WatermarkStrategy


def test_timestamp_assigner_returns_event_timestamp():
    timestamp = datetime(
        2026,
        9,
        12,
        22,
        0,
        tzinfo=timezone.utc,
    )

    event = {
        "event_timestamp": timestamp.isoformat(),
    }

    assigner = EventTimestampAssigner()

    assert assigner.extract_timestamp(event) == timestamp


def test_timestamp_assigner_accepts_datetime():
    timestamp = datetime(
        2026,
        9,
        12,
        22,
        0,
        tzinfo=timezone.utc,
    )

    event = {
        "event_timestamp": timestamp,
    }

    assigner = EventTimestampAssigner()

    assert assigner.extract_timestamp(event) == timestamp


def test_watermark_strategy_detects_late_event():
    strategy = WatermarkStrategy(
        max_out_of_orderness=timedelta(seconds=10),
    )

    event_time = datetime(
        2026,
        9,
        12,
        22,
        0,
        tzinfo=timezone.utc,
    )

    current_watermark = event_time + timedelta(seconds=20)

    assert strategy.is_late(event_time, current_watermark)


def test_watermark_strategy_accepts_event_within_allowed_lateness():
    strategy = WatermarkStrategy(
        max_out_of_orderness=timedelta(seconds=10),
    )

    event_time = datetime(
        2026,
        9,
        12,
        22,
        0,
        tzinfo=timezone.utc,
    )

    current_watermark = event_time + timedelta(seconds=5)

    assert not strategy.is_late(event_time, current_watermark)


def test_watermark_strategy_generates_monotonic_watermarks():
    strategy = WatermarkStrategy(
        max_out_of_orderness=timedelta(seconds=10),
    )

    first_event = datetime(
        2026,
        9,
        12,
        22,
        0,
        tzinfo=timezone.utc,
    )

    second_event = first_event + timedelta(seconds=30)

    first_watermark = strategy.generate_watermark(first_event)
    second_watermark = strategy.generate_watermark(second_event)

    assert second_watermark >= first_watermark