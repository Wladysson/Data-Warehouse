from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class Order:

    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    unit_price: float
    status: str
    created_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.order_id.strip():
            raise ValueError(
                "order_id não pode ser vazio."
            )

        if not self.customer_id.strip():
            raise ValueError(
                "customer_id não pode ser vazio."
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

        if not self.status.strip():
            raise ValueError(
                "status não pode ser vazio."
            )

        if self.created_at.tzinfo is None:
            raise ValueError(
                "created_at deve possuir timezone."
            )

    @property
    def total_amount(self) -> float:

        return self.quantity * self.unit_price

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

        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "product_id": self.product_id,
            "quantity": self.quantity,
            "unit_price": self.unit_price,
            "total_amount": self.total_amount,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "metadata": dict(self.metadata),
        }