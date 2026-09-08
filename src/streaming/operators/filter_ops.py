from __future__ import annotations

from typing import Iterable, Iterator, Optional, Set

from src.streaming.models import StreamingEvent


SUPPORTED_EVENT_TYPES = frozenset(
    {
        "click",
        "cart",
        "order",
        "delivery",
    }
)


def filter_valid_events(
    events: Iterable[StreamingEvent],
) -> Iterator[StreamingEvent]:

    for event in events:
        if not isinstance(event, StreamingEvent):
            continue

        if not event.event_id:
            continue

        if event.event_type not in SUPPORTED_EVENT_TYPES:
            continue

        if event.event_timestamp is None:
            continue

        if event.ingestion_timestamp is None:
            continue

        if not event.customer_id:
            continue

        yield event


def filter_by_event_type(
    events: Iterable[StreamingEvent],
    event_types: Optional[Set[str]] = None,
) -> Iterator[StreamingEvent]:

    allowed_types = (
        SUPPORTED_EVENT_TYPES
        if event_types is None
        else frozenset(event_types)
    )

    invalid_types = allowed_types.difference(
        SUPPORTED_EVENT_TYPES
    )

    if invalid_types:
        raise ValueError(
            "Tipos de evento não suportados: "
            f"{sorted(invalid_types)}"
        )

    for event in events:
        if (
            isinstance(event, StreamingEvent)
            and event.event_type in allowed_types
        ):
            yield event


def filter_click_events(
    events: Iterable[StreamingEvent],
) -> Iterator[StreamingEvent]:
    return filter_by_event_type(
        events,
        {"click"},
    )


def filter_cart_events(
    events: Iterable[StreamingEvent],
) -> Iterator[StreamingEvent]:
    return filter_by_event_type(
        events,
        {"cart"},
    )


def filter_order_events(
    events: Iterable[StreamingEvent],
) -> Iterator[StreamingEvent]:
    return filter_by_event_type(
        events,
        {"order"},
    )


def filter_delivery_events(
    events: Iterable[StreamingEvent],
) -> Iterator[StreamingEvent]:
    return filter_by_event_type(
        events,
        {"delivery"},
    )


def filter_by_customer(
    events: Iterable[StreamingEvent],
    customer_id: str,
) -> Iterator[StreamingEvent]:

    normalized_customer_id = customer_id.strip()

    if not normalized_customer_id:
        raise ValueError(
            "customer_id não pode ser vazio."
        )

    for event in events:
        if (
            isinstance(event, StreamingEvent)
            and event.customer_id == normalized_customer_id
        ):
            yield event


def filter_by_product(
    events: Iterable[StreamingEvent],
    product_id: str,
) -> Iterator[StreamingEvent]:

    normalized_product_id = product_id.strip()

    if not normalized_product_id:
        raise ValueError(
            "product_id não pode ser vazio."
        )

    for event in events:
        if (
            isinstance(event, StreamingEvent)
            and event.product_id == normalized_product_id
        ):
            yield event