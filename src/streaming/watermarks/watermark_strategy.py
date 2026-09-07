from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from src.streaming.models import StreamingEvent

from .timestamp_assigner import TimestampAssigner


@dataclass(frozen=True, slots=True)
class Watermark:

    timestamp: datetime

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError(
                "O timestamp do Watermark deve possuir timezone."
            )

    def is_after(
        self,
        timestamp: datetime,
    ) -> bool:
        if timestamp.tzinfo is None:
            raise ValueError(
                "timestamp deve possuir timezone."
            )

        return self.timestamp >= timestamp


class WatermarkStrategy:

    def __init__(
        self,
        out_of_orderness_seconds: int = 10,
        timestamp_assigner: Optional[
            TimestampAssigner
        ] = None,
    ) -> None:
        if out_of_orderness_seconds < 0:
            raise ValueError(
                "out_of_orderness_seconds "
                "deve ser maior ou igual a zero."
            )

        self.out_of_orderness_seconds = (
            out_of_orderness_seconds
        )

        self.timestamp_assigner = (
            timestamp_assigner
            or TimestampAssigner()
        )

        self._max_event_timestamp: Optional[
            datetime
        ] = None

        self._current_watermark: Optional[
            Watermark
        ] = None

    @property
    def max_event_timestamp(
        self,
    ) -> Optional[datetime]:
        return self._max_event_timestamp

    @property
    def current_watermark(
        self,
    ) -> Optional[Watermark]:
        return self._current_watermark

    @property
    def out_of_orderness_duration(
        self,
    ) -> timedelta:
        return timedelta(
            seconds=self.out_of_orderness_seconds
        )

    def observe(
        self,
        event: StreamingEvent,
    ) -> Watermark:

        timestamp = (
            self.timestamp_assigner.extract_timestamp(
                event
            )
        )

        if (
            self._max_event_timestamp is None
            or timestamp > self._max_event_timestamp
        ):
            self._max_event_timestamp = timestamp

        watermark_timestamp = (
            self._max_event_timestamp
            - self.out_of_orderness_duration
        )

        if (
            self._current_watermark is None
            or watermark_timestamp
            > self._current_watermark.timestamp
        ):
            self._current_watermark = Watermark(
                timestamp=watermark_timestamp
            )

        return self._current_watermark

    def observe_many(
        self,
        events: Iterable[StreamingEvent],
    ) -> Optional[Watermark]:

        watermark = self._current_watermark

        for event in events:
            watermark = self.observe(event)

        return watermark

    def calculate_watermark(
        self,
        max_event_timestamp: datetime,
    ) -> Watermark:

        if max_event_timestamp.tzinfo is None:
            raise ValueError(
                "max_event_timestamp deve possuir timezone."
            )

        return Watermark(
            timestamp=(
                max_event_timestamp
                - self.out_of_orderness_duration
            )
        )

    def is_late(
        self,
        event: StreamingEvent,
    ) -> bool:

        if self._current_watermark is None:
            return False

        event_timestamp = (
            self.timestamp_assigner.extract_timestamp(
                event
            )
        )

        return (
            event_timestamp
            < self._current_watermark.timestamp
        )

    def is_within_allowed_lateness(
        self,
        event: StreamingEvent,
        allowed_lateness_seconds: int,
    ) -> bool:

        if allowed_lateness_seconds < 0:
            raise ValueError(
                "allowed_lateness_seconds "
                "deve ser maior ou igual a zero."
            )

        if self._current_watermark is None:
            return True

        event_timestamp = (
            self.timestamp_assigner.extract_timestamp(
                event
            )
        )

        boundary = (
            self._current_watermark.timestamp
            - timedelta(
                seconds=allowed_lateness_seconds
            )
        )

        return event_timestamp >= boundary

    def reset(self) -> None:

        self._max_event_timestamp = None
        self._current_watermark = None

    def describe(self) -> dict[str, object]:
        return {
            "type": "bounded_out_of_orderness",
            "out_of_orderness_seconds": (
                self.out_of_orderness_seconds
            ),
            "max_event_timestamp": (
                self._max_event_timestamp.isoformat()
                if self._max_event_timestamp
                else None
            ),
            "current_watermark": (
                self._current_watermark.timestamp.isoformat()
                if self._current_watermark
                else None
            ),
        }