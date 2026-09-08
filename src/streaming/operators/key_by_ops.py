from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, Iterable, Iterator, List, Optional

from src.streaming.models import StreamingEvent


KeyFunction = Callable[
    [StreamingEvent],
    str,
]


def _validate_event(
    event: StreamingEvent,
) -> None:
    if not isinstance(event, StreamingEvent):
        raise TypeError(
            "event deve ser uma instância de StreamingEvent."
        )


def customer_key(
    event: StreamingEvent,
) -> str:
    _validate_event(event)

    if not event.customer_id:
        raise ValueError(
            "O evento não possui customer_id."
        )

    return event.customer_id


def product_key(
    event: StreamingEvent,
) -> str:
    _validate_event(event)

    if not event.product_id:
        raise ValueError(
            "O evento não possui product_id."
        )

    return event.product_id


def event_type_key(
    event: StreamingEvent,
) -> str:
    _validate_event(event)

    if not event.event_type:
        raise ValueError(
            "O evento não possui event_type."
        )

    return event.event_type


def order_key(
    event: StreamingEvent,
) -> str:
    _validate_event(event)

    if not event.order_id:
        raise ValueError(
            "O evento não possui order_id."
        )

    return event.order_id


def key_by(
    events: Iterable[StreamingEvent],
    key_function: KeyFunction,
) -> Dict[str, List[StreamingEvent]]:

    if not callable(key_function):
        raise TypeError(
            "key_function deve ser uma função chamável."
        )

    grouped: Dict[
        str,
        List[StreamingEvent],
    ] = defaultdict(list)

    for event in events:
        key = key_function(event)

        if not key:
            raise ValueError(
                "A função de chave retornou uma chave vazia."
            )

        grouped[key].append(event)

    return dict(grouped)


def key_by_customer(
    events: Iterable[StreamingEvent],
) -> Dict[str, List[StreamingEvent]]:
    return key_by(
        events,
        customer_key,
    )


def key_by_product(
    events: Iterable[StreamingEvent],
) -> Dict[str, List[StreamingEvent]]:
    return key_by(
        events,
        product_key,
    )


def key_by_event_type(
    events: Iterable[StreamingEvent],
) -> Dict[str, List[StreamingEvent]]:
    return key_by(
        events,
        event_type_key,
    )


def key_by_order(
    events: Iterable[StreamingEvent],
) -> Dict[str, List[StreamingEvent]]:
    return key_by(
        events,
        order_key,
    )


def iter_keyed_events(
    events: Iterable[StreamingEvent],
    key_function: KeyFunction,
) -> Iterator[tuple[str, StreamingEvent]]:


    if not callable(key_function):
        raise TypeError(
            "key_function deve ser uma função chamável."
        )

    for event in events:
        key = key_function(event)

        if not key:
            raise ValueError(
                "A função de chave retornou uma chave vazia."
            )

        yield key, event