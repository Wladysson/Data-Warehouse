from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .base_event import BaseEvent


@dataclass(frozen=True, slots=True)
class ClickEvent(BaseEvent):

    product_id: str = ""
    page: str = ""
    action: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()

        if self.event_type.lower() != "click":
            raise ValueError(
                "ClickEvent deve possuir event_type='click'."
            )

        if not self.product_id.strip():
            raise ValueError(
                "product_id não pode ser vazio."
            )

        if not self.page.strip():
            raise ValueError(
                "page não pode ser vazia."
            )

        if not self.action.strip():
            raise ValueError(
                "action não pode ser vazia."
            )

    @property
    def is_product_interaction(self) -> bool:

        return self.action.lower() in {
            "view",
            "click",
            "select",
            "details",
            "open",
        }

    @property
    def has_conversion_intent(self) -> bool:

        return self.action.lower() in {
            "buy",
            "checkout",
            "add_to_cart",
            "purchase",
        }

    def to_dict(self) -> dict[str, Any]:

        data = super().to_dict()

        data.update(
            {
                "product_id": self.product_id,
                "page": self.page,
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
            "product_id": self.product_id,
            "page": self.page,
            "action": self.action,
            "metadata": dict(self.metadata),
        }