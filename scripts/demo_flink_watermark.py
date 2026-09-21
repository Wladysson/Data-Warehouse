from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(
        0,
        str(ROOT_DIR)
    )


from datetime import datetime, timezone

from src.streaming.models import StreamingEvent
from src.streaming.watermarks.timestamp_assigner import TimestampAssigner
from src.streaming.windows.sliding_windows import SlidingWindowProcessor



def create_event(
    event_id: str,
    event_timestamp: str,
    product_id: str,
):
    return StreamingEvent(
        event_id=event_id,
        event_type="product_click",
        event_timestamp=datetime.fromisoformat(
            event_timestamp
        ),
        ingestion_timestamp=datetime(
            2026,
            1,
            1,
            12,
            1,
            0,
            tzinfo=timezone.utc,
        ),
        customer_id="customer_demo",
        session_id="session_demo",
        product_id=product_id,
        quantity=1,
        metadata={
            "source": "demo"
        },
    )


def main():

    print("=" * 60)
    print(" DEMONSTRAÇÃO FLINK - EVENT TIME + WATERMARK + SLIDING WINDOW ")
    print("=" * 60)

    assigner = TimestampAssigner()

    processor = SlidingWindowProcessor(
        size_seconds=60,
        slide_seconds=10,
        allowed_lateness_seconds=10,
    )


    print("\n1) EVENTOS RECEBIDOS")
    print("-" * 60)


    event_normal = create_event(
        "event-001",
        "2026-01-01T12:00:10+00:00",
        "product-100",
    )


    event_late = create_event(
        "event-002",
        "2026-01-01T12:00:05+00:00",
        "product-200",
    )


    print(
        f"Evento normal: {event_normal.event_timestamp}"
    )

    print(
        f"Evento atrasado: {event_late.event_timestamp}"
    )


    print("\n2) EVENT TIME")
    print("-" * 60)


    print(
        "Timestamp usado pelo processamento:"
    )

    print(
        assigner.extract_timestamp(
            event_normal
        )
    )


    print("\n3) ATRASO DOS EVENTOS")
    print("-" * 60)


    print(
        "Atraso evento normal:",
        assigner.calculate_event_delay(
            event_normal
        ),
        "segundos",
    )


    print(
        "Atraso evento atrasado:",
        assigner.calculate_event_delay(
            event_late
        ),
        "segundos",
    )


    print("\n4) WATERMARK")
    print("-" * 60)


    watermark = datetime(
        2026,
        1,
        1,
        12,
        1,
        0,
        tzinfo=timezone.utc,
    )


    print(
        "Watermark atual:",
        watermark,
    )


    print(
        "Evento atrasado?",
        processor.is_late_event(
            event_late,
            watermark,
        ),
    )


    print("\n5) SLIDING WINDOW")
    print("-" * 60)


    events = [
        event_late,
        event_normal,
    ]


    aggregations = processor.process(
        events
    )


    for aggregation in aggregations:

        if (
        aggregation.window_start.hour == 12
        and aggregation.window_start.minute == 0
    ):
            print(
            "\n>>> JANELA PRINCIPAL PARA DEMONSTRAÇÃO <<<"
        )
    else:
        print(
            "\nJanela processada:"
        )

        print(
            "Inicio:",
            aggregation.window_start,
        )

        print(
            "Fim:",
            aggregation.window_end,
        )

        print(
            "Eventos:",
            aggregation.event_count,
        )

        print(
            "Clientes:",
            aggregation.unique_customers,
        )


    print("\n")
    print("=" * 60)
    print(" DEMONSTRAÇÃO FINALIZADA ")
    print("=" * 60)


if __name__ == "__main__":
    main()
