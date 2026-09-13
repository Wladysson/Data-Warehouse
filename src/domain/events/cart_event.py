from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .base_event import BaseEvent


@dataclass(frozen=True, slots=True)
class CartEvent(BaseEvent):

    cart_id: str = ""
    product_id: str = ""
    quantity: int = 0
    action: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()

        if self.event_type.lower() != "cart":
            raise ValueError(
                "CartEvent deve possuir event_type='cart'."
            )

        if not self.cart_id.strip():
            raise ValueError(
                "cart_id não pode ser vazio."
            )

        if not self.product_id.strip():
            raise ValueError(
                "product_id não pode ser vazio."
            )

        if self.quantity <= 0:
            raise ValueError(
                "quantity deve ser maior que zero."
            )

        if not self.action.strip():
            raise ValueError(
                "action não pode ser vazia."
            )

    @property
    def is_addition(self) -> bool:

        return self.action.lower() in {
            "add",
            "add_to_cart",
            "insert",
        }

    @property
    def is_removal(self) -> bool:

        return self.action.lower() in {
            "remove",
            "remove_from_cart",
            "delete",
        }

    @property
    def is_update(self) -> bool:

        return self.action.lower() in {
            "update",
            "change_quantity",
            "quantity_update",
        }

    @property
    def is_abandonment_signal(self) -> bool:

        return self.action.lower() in {
            "abandon",
            "abandoned",
            "timeout",
        }

    def to_dict(self) -> dict[str, Any]:

        data = super().to_dict()

        data.update(
            {
                "cart_id": self.cart_id,
                "product_id": self.product_id,
                "quantity": self.quantity,
                "action": self.action,
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
            "cart_id": self.cart_id,
            "product_id": self.product_id,
            "quantity": self.quantity,
            "action": self.action,
            "metadata": dict(self.metadata),
        }