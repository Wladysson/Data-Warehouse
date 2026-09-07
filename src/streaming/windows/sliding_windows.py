from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Optional

from src.streaming.models import (
    StreamingEvent,
    WindowAggregation,
)

from .window_utils import (
    calculate_window_end,
    generate_window_starts,
    validate_window_configuration,
)


@dataclass(frozen=True, slots=True)
class SlidingWindow:

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            raise ValueError(
                "start deve possuir timezone."
            )

        if self.end.tzinfo is None:
            raise ValueError(
                "end deve possuir timezone."
            )

        if self.end <= self.start:
            raise ValueError(
                "end deve ser posterior a start."
            )

    @property
    def duration_seconds(self) -> float:
        return (
            self.end - self.start
        ).total_seconds()

    def contains(
        self,
        timestamp: datetime,
    ) -> bool:
        if timestamp.tzinfo is None:
            raise ValueError(
                "timestamp deve possuir timezone."
            )

        return (
            self.start
            <= timestamp
            < self.end
        )


class SlidingWindowProcessor:

    def __init__(
        self,
        size_seconds: int = 60,
        slide_seconds: int = 10,
        allowed_lateness_seconds: int = 10,
    ) -> None:
        validate_window_configuration(
            size_seconds,
            slide_seconds,
        )

        if allowed_lateness_seconds < 0:
            raise ValueError(
                "allowed_lateness_seconds "
                "deve ser maior ou igual a zero."
            )

        self.size_seconds = size_seconds
        self.slide_seconds = slide_seconds
        self.allowed_lateness_seconds = (
            allowed_lateness_seconds
        )

    @property
    def window_duration(self) -> timedelta:
        return timedelta(
            seconds=self.size_seconds
        )

    @property
    def slide_duration(self) -> timedelta:
        return timedelta(
            seconds=self.slide_seconds
        )

    def create_window(
        self,
        window_start: datetime,
    ) -> SlidingWindow:
        return SlidingWindow(
            start=window_start,
            end=calculate_window_end(
                window_start,
                self.size_seconds,
            ),
        )

    def windows_for_event(
        self,
        event: StreamingEvent,
    ) -> List[SlidingWindow]:

        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "event deve ser uma instância de StreamingEvent."
            )

        starts = generate_window_starts(
            event_timestamp=event.event_timestamp,
            window_size_seconds=self.size_seconds,
            window_slide_seconds=self.slide_seconds,
        )

        return [
            self.create_window(start)
            for start in starts
        ]

    def assign_events(
        self,
        events: Iterable[StreamingEvent],
    ) -> Dict[SlidingWindow, List[StreamingEvent]]:

        windows: Dict[
            SlidingWindow,
            List[StreamingEvent],
        ] = defaultdict(list)

        for event in events:
            if not isinstance(event, StreamingEvent):
                raise TypeError(
                    "Todos os elementos devem ser StreamingEvent."
                )

            for window in self.windows_for_event(event):
                windows[window].append(event)

        return dict(windows)

    def assign_events_by_key(
        self,
        events: Iterable[StreamingEvent],
        key_field: str = "customer_id",
    ) -> Dict[
        str,
        Dict[SlidingWindow, List[StreamingEvent]],
    ]:

        if not key_field.strip():
            raise ValueError(
                "key_field não pode ser vazio."
            )

        result: Dict[
            str,
            Dict[SlidingWindow, List[StreamingEvent]],
        ] = defaultdict(
            lambda: defaultdict(list)
        )

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

            if key is None:
                raise ValueError(
                    f"O evento não possui o campo '{key_field}'."
                )

            if not isinstance(key, str) or not key.strip():
                raise ValueError(
                    f"A chave '{key_field}' é inválida."
                )

            for window in self.windows_for_event(event):
                result[key][window].append(event)

        return {
            key: dict(window_map)
            for key, window_map in result.items()
        }

    def aggregate_window(
        self,
        window: SlidingWindow,
        events: Iterable[StreamingEvent],
        key: Optional[str] = None,
    ) -> WindowAggregation:

        event_list = list(events)

        if not event_list:
            raise ValueError(
                "Não é possível agregar uma janela vazia."
            )

        for event in event_list:
            if not isinstance(event, StreamingEvent):
                raise TypeError(
                    "Todos os elementos devem ser StreamingEvent."
                )

            if not window.contains(
                event.event_timestamp
            ):
                raise ValueError(
                    "O evento não pertence à janela informada."
                )

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

        from decimal import Decimal

        total_amount = sum(
            (
                event.total_amount
                or Decimal("0.00")
                for event in event_list
            ),
            Decimal("0.00"),
        )

        return WindowAggregation(
            window_start=window.start,
            window_end=window.end,
            event_type=event_type,
            event_count=len(event_list),
            unique_customers=unique_customers,
            total_quantity=total_quantity,
            total_amount=total_amount.quantize(
                Decimal("0.01")
            ),
            key=key,
            metadata={
                "window_type": "sliding",
                "window_size_seconds": (
                    self.size_seconds
                ),
                "window_slide_seconds": (
                    self.slide_seconds
                ),
            },
        )

    def process(
        self,
        events: Iterable[StreamingEvent],
    ) -> List[WindowAggregation]:

        assigned_windows = self.assign_events(
            events
        )

        aggregations: List[
            WindowAggregation
        ] = []

        for window, window_events in sorted(
            assigned_windows.items(),
            key=lambda item: item[0].start,
        ):
            aggregations.append(
                self.aggregate_window(
                    window,
                    window_events,
                )
            )

        return aggregations

    def process_by_key(
        self,
        events: Iterable[StreamingEvent],
        key_field: str = "customer_id",
    ) -> List[WindowAggregation]:

        keyed_windows = self.assign_events_by_key(
            events,
            key_field=key_field,
        )

        aggregations: List[
            WindowAggregation
        ] = []

        for key, windows in keyed_windows.items():
            for window, window_events in sorted(
                windows.items(),
                key=lambda item: item[0].start,
            ):
                aggregations.append(
                    self.aggregate_window(
                        window,
                        window_events,
                        key=key,
                    )
                )

        return aggregations

    def is_late_event(
        self,
        event: StreamingEvent,
        current_watermark: datetime,
    ) -> bool:

        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "event deve ser uma instância de StreamingEvent."
            )

        if current_watermark.tzinfo is None:
            raise ValueError(
                "current_watermark deve possuir timezone."
            )

        allowed_boundary = (
            current_watermark
            - timedelta(
                seconds=self.allowed_lateness_seconds
            )
        )

        return event.event_timestamp < allowed_boundary

    def iter_windows(
        self,
        events: Iterable[StreamingEvent],
    ) -> Iterator[
        tuple[SlidingWindow, List[StreamingEvent]]
    ]:

        assigned = self.assign_events(events)

        for window, window_events in sorted(
            assigned.items(),
            key=lambda item: item[0].start,
        ):
            yield window, window_events