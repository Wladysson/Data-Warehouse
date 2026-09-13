from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class Delivery:


    delivery_id: str
    order_id: str
    status: str
    carrier: str
    estimated_delivery: datetime
    created_at: datetime
    actual_delivery: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.delivery_id.strip():
            raise ValueError(
                "delivery_id não pode ser vazio."
            )

        if not self.order_id.strip():
            raise ValueError(
                "order_id não pode ser vazio."
            )

        if not self.status.strip():
            raise ValueError(
                "status não pode ser vazio."
            )

        if not self.carrier.strip():
            raise ValueError(
                "carrier não pode ser vazia."
            )

        if self.estimated_delivery.tzinfo is None:
            raise ValueError(
                "estimated_delivery deve possuir timezone."
            )

        if self.created_at.tzinfo is None:
            raise ValueError(
                "created_at deve possuir timezone."
            )

        if (
            self.actual_delivery is not None
            and self.actual_delivery.tzinfo is None
        ):
            raise ValueError(
                "actual_delivery deve possuir timezone."
            )

        if self.estimated_delivery < self.created_at:
            raise ValueError(
                "estimated_delivery não pode ser anterior a created_at."
            )

        if (
            self.actual_delivery is not None
            and self.actual_delivery < self.created_at
        ):
            raise ValueError(
                "actual_delivery não pode ser anterior a created_at."
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

        if self.actual_delivery is None:
            return False

        return self.actual_delivery > self.estimated_delivery

    @property
    def delay_seconds(self) -> float:

        if self.actual_delivery is None:
            return 0.0

        return (
            self.actual_delivery - self.estimated_delivery
        ).total_seconds()

    @property
    def delivery_duration_seconds(self) -> float:

        end_time = self.actual_delivery

        if end_time is None:
            return 0.0

        return (
            end_time - self.created_at
        ).total_seconds()

    def to_dict(self) -> dict[str, Any]:

        return {
            "delivery_id": self.delivery_id,
            "order_id": self.order_id,
            "status": self.status,
            "carrier": self.carrier,
            "estimated_delivery": self.estimated_delivery.isoformat(),
            "created_at": self.created_at.isoformat(),
            "actual_delivery": (
                self.actual_delivery.isoformat()
                if self.actual_delivery is not None
                else None
            ),
            "metadata": dict(self.metadata),
        }