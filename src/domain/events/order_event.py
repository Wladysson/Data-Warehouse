from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .base_event import BaseEvent


@dataclass(frozen=True, slots=True)
class OrderEvent(BaseEvent):

    order_id: str = ""
    product_id: str = ""
    quantity: int = 0
    unit_price: float = 0.0
    total_amount: float = 0.0
    status: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()

        if self.event_type.lower() != "order":
            raise ValueError(
                "OrderEvent deve possuir event_type='order'."
            )

        if not self.order_id.strip():
            raise ValueError(
                "order_id não pode ser vazio."
            )

        if not self.product_id.strip():
            raise ValueError(
                "product_id não pode ser vazio."
            )

        if self.quantity <= 0:
            raise ValueError(
                "quantity deve ser maior que zero."
            )

        if self.unit_price < 0:
            raise ValueError(
                "unit_price não pode ser negativo."
            )

        if self.total_amount < 0:
            raise ValueError(
                "total_amount não pode ser negativo."
            )

        expected_total = self.quantity * self.unit_price

        if abs(self.total_amount - expected_total) > 0.01:
            raise ValueError(
                "total_amount deve corresponder à quantidade multiplicada "
                "pelo preço unitário."
            )

        if not self.status.strip():
            raise ValueError(
                "status não pode ser vazio."
            )

    @property
    def is_confirmed(self) -> bool:

        return self.status.lower() in {
            "confirmed",
            "confirmado",
        }

    @property
    def is_processing(self) -> bool:

        return self.status.lower() in {
            "processing",
            "processando",
        }

    @property
    def is_completed(self) -> bool:

        return self.status.lower() in {
            "completed",
            "complete",
            "concluido",
            "concluído",
            "delivered",
            "entregue",
        }

    @property
    def is_cancelled(self) -> bool:

        return self.status.lower() in {
            "cancelled",
            "canceled",
            "cancelado",
        }

    @property
    def is_high_value(self) -> bool:

        return self.total_amount >= 10000.0

    def to_dict(self) -> dict[str, Any]:

        data = super().to_dict()

        data.update(
            {
                "order_id": self.order_id,
                "product_id": self.product_id,
                "quantity": self.quantity,
                "unit_price": self.unit_price,
                "total_amount": self.total_amount,
                "status": self.status,
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
            "product_id": self.product_id,
            "quantity": self.quantity,
            "unit_price": self.unit_price,
            "total_amount": self.total_amount,
            "status": self.status,
            "metadata": dict(self.metadata),
        }