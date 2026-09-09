from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class BaseEvent:

    event_id: str
    event_type: str
    event_timestamp: datetime
    ingestion_timestamp: datetime
    customer_id: str
    session_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.event_id.strip():
            raise ValueError("event_id não pode ser vazio.")

        if not self.event_type.strip():
            raise ValueError("event_type não pode ser vazio.")

        if not self.customer_id.strip():
            raise ValueError("customer_id não pode ser vazio.")

        if self.event_timestamp.tzinfo is None:
            raise ValueError(
                "event_timestamp deve possuir timezone."
            )

        if self.ingestion_timestamp.tzinfo is None:
            raise ValueError(
                "ingestion_timestamp deve possuir timezone."
            )

        if self.session_id is not None and not self.session_id.strip():
            raise ValueError(
                "session_id não pode ser vazio quando informado."
            )

        if self.ingestion_timestamp < self.event_timestamp:
            raise ValueError(
                "ingestion_timestamp não pode ser anterior "
                "ao event_timestamp."
            )

    @property
    def event_delay_seconds(self) -> float:

        return (
            self.ingestion_timestamp - self.event_timestamp
        ).total_seconds()

    @property
    def is_late_event(self) -> bool:

        return self.event_delay_seconds > 0

    def to_dict(self) -> dict[str, Any]:

        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_timestamp": self.event_timestamp.isoformat(),
            "ingestion_timestamp": self.ingestion_timestamp.isoformat(),
            "customer_id": self.customer_id,
            "session_id": self.session_id,
            "metadata": dict(self.metadata),
        }

    def with_metadata(
        self,
        **metadata: Any,
    ) -> "BaseEvent":

        merged_metadata = {
            **self.metadata,
            **metadata,
        }

        return BaseEvent(
            event_id=self.event_id,
            event_type=self.event_type,
            event_timestamp=self.event_timestamp,
            ingestion_timestamp=self.ingestion_timestamp,
            customer_id=self.customer_id,
            session_id=self.session_id,
            metadata=merged_metadata,
        )