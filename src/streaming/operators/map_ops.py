from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, Iterator, Optional

from src.streaming.models import StreamingEvent


EventMapper = Callable[
    [StreamingEvent],
    StreamingEvent,
]


def normalize_event(
    event: StreamingEvent,
) -> StreamingEvent:

    if not isinstance(event, StreamingEvent):
        raise TypeError(
            "event deve ser uma instância de StreamingEvent."
        )

    normalized_customer_id = event.customer_id.strip()
    normalized_event_type = event.event_type.strip().lower()

    if not normalized_customer_id:
        raise ValueError(
            "customer_id não pode ser vazio."
        )

    if not normalized_event_type:
        raise ValueError(
            "event_type não pode ser vazio."
        )

    return StreamingEvent(
        event_id=event.event_id.strip(),
        event_type=normalized_event_type,
        event_timestamp=event.event_timestamp,
        ingestion_timestamp=event.ingestion_timestamp,
        customer_id=normalized_customer_id,
        session_id=(
            event.session_id.strip()
            if event.session_id
            else None
        ),
        product_id=(
            event.product_id.strip()
            if event.product_id
            else None
        ),
        order_id=(
            event.order_id.strip()
            if event.order_id
            else None
        ),
        cart_id=(
            event.cart_id.strip()
            if event.cart_id
            else None
        ),
        delivery_id=(
            event.delivery_id.strip()
            if event.delivery_id
            else None
        ),
        quantity=event.quantity,
        unit_price=event.unit_price,
        total_amount=event.total_amount,
        status=(
            event.status.strip().lower()
            if event.status
            else None
        ),
        metadata=dict(event.metadata),
    )


def enrich_event(
    event: StreamingEvent,
    metadata: Optional[Dict[str, Any]] = None,
) -> StreamingEvent:

    if not isinstance(event, StreamingEvent):
        raise TypeError(
            "event deve ser uma instância de StreamingEvent."
        )

    enriched_metadata = dict(event.metadata)

    if metadata:
        enriched_metadata.update(metadata)

    enriched_metadata.setdefault(
        "processed_at",
        datetime.now(timezone.utc).isoformat(),
    )

    return StreamingEvent(
        event_id=event.event_id,
        event_type=event.event_type,
        event_timestamp=event.event_timestamp,
        ingestion_timestamp=event.ingestion_timestamp,
        customer_id=event.customer_id,
        session_id=event.session_id,
        product_id=event.product_id,
        order_id=event.order_id,
        cart_id=event.cart_id,
        delivery_id=event.delivery_id,
        quantity=event.quantity,
        unit_price=event.unit_price,
        total_amount=event.total_amount,
        status=event.status,
        metadata=enriched_metadata,
    )


def map_to_dict(
    event: StreamingEvent,
) -> Dict[str, Any]:

    if not isinstance(event, StreamingEvent):
        raise TypeError(
            "event deve ser uma instância de StreamingEvent."
        )

    return event.to_dict()


def map_events(
    events: Iterable[StreamingEvent],
    mapper: EventMapper,
) -> Iterator[StreamingEvent]:
    
    if not callable(mapper):
        raise TypeError(
            "mapper deve ser uma função chamável."
        )

    for event in events:
        mapped_event = mapper(event)

        if not isinstance(mapped_event, StreamingEvent):
            raise TypeError(
                "O mapper deve retornar StreamingEvent."
            )

        yield mapped_event


def calculate_event_value(
    event: StreamingEvent,
) -> Decimal:

    if not isinstance(event, StreamingEvent):
        raise TypeError(
            "event deve ser uma instância de StreamingEvent."
        )

    if (
        event.quantity is None
        or event.unit_price is None
    ):
        return Decimal("0.00")

    return (
        event.unit_price
        * Decimal(event.quantity)
    ).quantize(
        Decimal("0.01")
    )