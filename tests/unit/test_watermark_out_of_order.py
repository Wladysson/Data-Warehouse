from datetime import datetime, timezone, timedelta

from src.streaming.models import StreamingEvent
from src.streaming.watermarks.timestamp_assigner import TimestampAssigner
from src.streaming.windows.sliding_windows import SlidingWindowProcessor


def create_event(
    event_id: str,
    event_time: datetime,
    ingestion_delay_seconds: int,
):
    return StreamingEvent(
        event_id=event_id,
        event_type="purchase",
        event_timestamp=event_time,
        ingestion_timestamp=(
            event_time
            + timedelta(seconds=ingestion_delay_seconds)
        ),
        customer_id="customer-001",
        session_id="session-001",
        product_id="product-001",
        quantity=1,
    )


def test_event_time_accepts_out_of_order_events():

    assigner = TimestampAssigner()

    base_time = datetime(
        2026,
        1,
        1,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    # Evento normal
    event_1 = create_event(
        "evt-001",
        base_time + timedelta(seconds=10),
        2,
    )

    # Evento chegou atrasado,
    # mas pertence ao passado
    event_2 = create_event(
        "evt-002",
        base_time + timedelta(seconds=5),
        15,
    )

    timestamp_1 = assigner.extract_epoch_millis(
        event_1
    )

    timestamp_2 = assigner.extract_epoch_millis(
        event_2
    )

    print("\nEVENT TIME:")
    print(
        "Evento 1:",
        event_1.event_timestamp,
    )
    print(
        "Evento atrasado:",
        event_2.event_timestamp,
    )

    print("\nATRASO:")
    print(
        assigner.calculate_event_delay(
            event_1
        ),
        "segundos",
    )

    print(
        assigner.calculate_event_delay(
            event_2
        ),
        "segundos",
    )

    assert timestamp_2 < timestamp_1


def test_watermark_detects_late_event():

    processor = SlidingWindowProcessor(
        size_seconds=60,
        slide_seconds=10,
        allowed_lateness_seconds=10,
    )

    watermark = datetime(
        2026,
        1,
        1,
        12,
        1,
        0,
        tzinfo=timezone.utc,
    )

    late_event = create_event(
        "evt-late",
        datetime(
            2026,
            1,
            1,
            11,
            59,
            30,
            tzinfo=timezone.utc,
        ),
        40,
    )

    result = processor.is_late_event(
        late_event,
        watermark,
    )

    print("\nWATERMARK:")
    print(
        "Atual:",
        watermark,
    )

    print(
        "Evento:",
        late_event.event_timestamp,
    )

    print(
        "Evento atrasado?",
        result,
    )

    assert result is True
