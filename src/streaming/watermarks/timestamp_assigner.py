from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from src.streaming.models import StreamingEvent


class TimestampAssigner:

    def extract_timestamp(
        self,
        event: StreamingEvent,
    ) -> datetime:
        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "event deve ser uma instância de StreamingEvent."
            )

        timestamp = event.event_timestamp

        if timestamp.tzinfo is None:
            raise ValueError(
                "event_timestamp deve possuir timezone."
            )

        return timestamp

    def extract_epoch_millis(
        self,
        event: StreamingEvent,
    ) -> int:
        timestamp = self.extract_timestamp(event)

        return int(
            timestamp.timestamp() * 1000
        )

    def validate_timestamp(
        self,
        event: StreamingEvent,
    ) -> bool:
        if not isinstance(event, StreamingEvent):
            return False

        if event.event_timestamp.tzinfo is None:
            return False

        if event.ingestion_timestamp.tzinfo is None:
            return False

        return (
            event.event_timestamp
            <= event.ingestion_timestamp
        )

    def calculate_event_delay(
        self,
        event: StreamingEvent,
    ) -> float:

        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "event deve ser uma instância de StreamingEvent."
            )

        delay = (
            event.ingestion_timestamp
            - event.event_timestamp
        ).total_seconds()

        return max(0.0, delay)

    def enrich_with_temporal_metadata(
        self,
        event: StreamingEvent,
    ) -> StreamingEvent:

        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "event deve ser uma instância de StreamingEvent."
            )

        metadata: Dict[str, Any] = dict(
            event.metadata
        )

        metadata["event_time_epoch_millis"] = (
            self.extract_epoch_millis(event)
        )

        metadata["event_delay_seconds"] = (
            self.calculate_event_delay(event)
        )

        metadata["event_time_timezone"] = (
            str(
                event.event_timestamp.tzinfo
            )
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
            metadata=metadata,
        )