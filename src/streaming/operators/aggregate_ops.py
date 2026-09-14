from __future__ import annotations

from decimal import Decimal
from typing import Dict, Iterable, Iterator, Optional

from src.streaming.models import (
    StreamingEvent,
    WindowAggregation,
)


def aggregate_events(
    events: Iterable[StreamingEvent],
    key: Optional[str] = None,
) -> WindowAggregation:
    event_list = list(events)

    if not event_list:
        raise ValueError(
            "Não é possível agregar uma coleção vazia."
        )

    for event in event_list:
        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "Todos os elementos devem ser StreamingEvent."
            )

    timestamps = [
        event.event_timestamp
        for event in event_list
    ]

    window_start = min(timestamps)
    window_end = max(timestamps)

    event_types = {
        event.event_type
        for event in event_list
    }

    event_type = (
        next(iter(event_types))
        if len(event_types) == 1
        else "mixed"
    )

    unique_customers = len(
        {
            event.customer_id
            for event in event_list
        }
    )

    total_quantity = sum(
        event.quantity or 0
        for event in event_list
    )

    total_amount = sum(
        (
            event.total_amount
            or Decimal("0.00")
            for event in event_list
        ),
        Decimal("0.00"),
    )

    return WindowAggregation(
        window_start=window_start,
        window_end=window_end,
        event_type=event_type,
        event_count=len(event_list),
        unique_customers=unique_customers,
        total_quantity=total_quantity,
        total_amount=total_amount.quantize(
            Decimal("0.01")
        ),
        key=key,
        metadata={
            "aggregation_type": "window",
            "event_types": sorted(event_types),
        },
    )


def aggregate_by_event_type(
    events: Iterable[StreamingEvent],
) -> Dict[str, WindowAggregation]:
    grouped: Dict[
        str,
        list[StreamingEvent],
    ] = {}

    for event in events:
        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "Todos os elementos devem ser StreamingEvent."
            )

        grouped.setdefault(
            event.event_type,
            [],
        ).append(event)

    return {
        event_type: aggregate_events(
            event_list,
            key=event_type,
        )
        for event_type, event_list in grouped.items()
    }


def aggregate_by_customer(
    events: Iterable[StreamingEvent],
) -> Dict[str, WindowAggregation]:
    grouped: Dict[
        str,
        list[StreamingEvent],
    ] = {}

    for event in events:
        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "Todos os elementos devem ser StreamingEvent."
            )

        if not event.customer_id:
            continue

        grouped.setdefault(
            event.customer_id,
            [],
        ).append(event)

    return {
        customer_id: aggregate_events(
            event_list,
            key=customer_id,
        )
        for customer_id, event_list in grouped.items()
    }


def aggregate_by_product(
    events: Iterable[StreamingEvent],
) -> Dict[str, WindowAggregation]:
    grouped: Dict[
        str,
        list[StreamingEvent],
    ] = {}

    for event in events:
        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "Todos os elementos devem ser StreamingEvent."
            )

        if not event.product_id:
            continue

        grouped.setdefault(
            event.product_id,
            [],
        ).append(event)

    return {
        product_id: aggregate_events(
            event_list,
            key=product_id,
        )
        for product_id, event_list in grouped.items()
    }


def iter_aggregations(
    events: Iterable[StreamingEvent],
    key_field: Optional[str] = None,
) -> Iterator[WindowAggregation]:
    if key_field is None:
        yield aggregate_events(events)
        return

    grouped: Dict[
        str,
        list[StreamingEvent],
    ] = {}

    for event in events:
        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "Todos os elementos devem ser StreamingEvent."
            )

        key = getattr(
            event,
            key_field,
            None,
        )

        if not isinstance(key, str) or not key.strip():
            continue

        grouped.setdefault(
            key,
            [],
        ).append(event)

    for key, event_list in grouped.items():
        yield aggregate_events(
            event_list,
            key=key,
        )


def calculate_total_value(
    events: Iterable[StreamingEvent],
) -> Decimal:
    total = sum(
        (
            event.total_amount
            or Decimal("0.00")
            for event in events
            if isinstance(event, StreamingEvent)
        ),
        Decimal("0.00"),
    )

    return total.quantize(
        Decimal("0.01")
    )


def count_events(
    events: Iterable[StreamingEvent],
) -> int:
    return sum(
        1
        for event in events
        if isinstance(event, StreamingEvent)
    )


def count_unique_customers(
    events: Iterable[StreamingEvent],
) -> int:
    return len(
        {
            event.customer_id
            for event in events
            if (
                isinstance(event, StreamingEvent)
                and event.customer_id
            )
        }
    )


def calculate_average_ticket(
    events: Iterable[StreamingEvent],
) -> Decimal:
    event_list = [
        event
        for event in events
        if (
            isinstance(event, StreamingEvent)
            and event.total_amount is not None
        )
    ]

    if not event_list:
        return Decimal("0.00")

    total = calculate_total_value(
        event_list
    )

    return (
        total / Decimal(len(event_list))
    ).quantize(
        Decimal("0.01")
    )