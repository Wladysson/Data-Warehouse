from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .base_event import BaseEvent


@dataclass(frozen=True, slots=True)
class DeliveryEvent(BaseEvent):

    order_id: str = ""
    delivery_id: str = ""
    status: str = ""
    carrier: str = ""
    estimated_delivery: datetime | None = None
    actual_delivery: datetime | None = None

    def __post_init__(self) -> None:
        super().__post_init__()

        if self.event_type.lower() != "delivery":
            raise ValueError(
                "DeliveryEvent deve possuir event_type='delivery'."
            )

        if not self.order_id.strip():
            raise ValueError(
                "order_id não pode ser vazio."
            )

        if not self.delivery_id.strip():
            raise ValueError(
                "delivery_id não pode ser vazio."
            )

        if not self.status.strip():
            raise ValueError(
                "status não pode ser vazio."
            )

        if not self.carrier.strip():
            raise ValueError(
                "carrier não pode ser vazia."
            )

        if (
            self.estimated_delivery is not None
            and self.estimated_delivery.tzinfo is None
        ):
            raise ValueError(
                "estimated_delivery deve possuir timezone."
            )

        if (
            self.actual_delivery is not None
            and self.actual_delivery.tzinfo is None
        ):
            raise ValueError(
                "actual_delivery deve possuir timezone."
            )

        if (
            self.actual_delivery is not None
            and self.estimated_delivery is not None
            and self.actual_delivery < self.estimated_delivery
        ):
            raise ValueError(
                "actual_delivery não pode ser anterior à "
                "estimated_delivery."
            )

    @property
    def is_delivered(self) -> bool:

        return self.status.lower() in {
            "delivered",
            "entregue",
            "completed",
            "complete",
        }

    @property
    def is_in_transit(self) -> bool:

        return self.status.lower() in {
            "in_transit",
            "in-transit",
            "transit",
            "em_transito",
            "em trânsito",
        }

    @property
    def is_pending(self) -> bool:

        return self.status.lower() in {
            "pending",
            "aguardando",
            "preparing",
            "preparando",
        }

    @property
    def is_cancelled(self) -> bool:

        return self.status.lower() in {
            "cancelled",
            "canceled",
            "cancelado",
        }

    @property
    def is_late(self) -> bool:

        if (
            self.actual_delivery is None
            or self.estimated_delivery is None
        ):
            return False

        return self.actual_delivery > self.estimated_delivery

    @property
    def delay_seconds(self) -> float:

        if (
            self.actual_delivery is None
            or self.estimated_delivery is None
        ):
            return 0.0

        return (
            self.actual_delivery - self.estimated_delivery
        ).total_seconds()

    def to_dict(self) -> dict[str, Any]:

        data = super().to_dict()

        data.update(
            {
                "order_id": self.order_id,
                "delivery_id": self.delivery_id,
                "status": self.status,
                "carrier": self.carrier,
                "estimated_delivery": (
                    self.estimated_delivery.isoformat()
                    if self.estimated_delivery is not None
                    else None
                ),
                "actual_delivery": (
                    self.actual_delivery.isoformat()
                    if self.actual_delivery is not None
                    else None
                ),
            }
        )

        return data

    def to_event_record(self) -> dict[str, Any]:

        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_timestamp": self.event_timestamp.isoformat(),
            "ingestion_timestamp": self.ingestion_timestamp.isoformat(),
            "customer_id": self.customer_id,
            "session_id": self.session_id,
            "order_id": self.order_id,
            "delivery_id": self.delivery_id,
            "status": self.status,
            "carrier": self.carrier,
            "estimated_delivery": (
                self.estimated_delivery.isoformat()
                if self.estimated_delivery is not None
                else None
            ),
            "actual_delivery": (
                self.actual_delivery.isoformat()
                if self.actual_delivery is not None
                else None
            ),
            "metadata": dict(self.metadata),
        }