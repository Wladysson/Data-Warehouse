from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class Cart:

    cart_id: str
    customer_id: str
    product_id: str
    quantity: int
    status: str
    created_at: datetime
    updated_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.cart_id.strip():
            raise ValueError(
                "cart_id não pode ser vazio."
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

        if not self.status.strip():
            raise ValueError(
                "status não pode ser vazio."
            )

        if self.created_at.tzinfo is None:
            raise ValueError(
                "created_at deve possuir timezone."
            )

        if (
            self.updated_at is not None
            and self.updated_at.tzinfo is None
        ):
            raise ValueError(
                "updated_at deve possuir timezone."
            )

        if (
            self.updated_at is not None
            and self.updated_at < self.created_at
        ):
            raise ValueError(
                "updated_at não pode ser anterior a created_at."
            )

    @property
    def is_active(self) -> bool:

        return self.status.lower() in {
            "active",
            "open",
            "aberto",
            "ativo",
        }

    @property
    def is_abandoned(self) -> bool:

        return self.status.lower() in {
            "abandoned",
            "abandonado",
            "expired",
            "expired",
        }

    @property
    def is_converted(self) -> bool:

        return self.status.lower() in {
            "converted",
            "convertido",
            "completed",
            "concluido",
            "concluído",
        }

    @property
    def is_empty(self) -> bool:

        return self.quantity <= 0

    def to_dict(self) -> dict[str, Any]:

        return {
            "cart_id": self.cart_id,
            "customer_id": self.customer_id,
            "product_id": self.product_id,
            "quantity": self.quantity,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "updated_at": (
                self.updated_at.isoformat()
                if self.updated_at is not None
                else None
            ),
            "metadata": dict(self.metadata),
        }