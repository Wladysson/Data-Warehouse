"""
Tipos de eventos processados pela pipeline de dados.
"""

from __future__ import annotations

from enum import StrEnum


class EventType(StrEnum):

    CLICK = "click"
    CART = "cart"
    ORDER = "order"
    DELIVERY = "delivery"

    @classmethod
    def values(cls) -> tuple[str, ...]:

        return tuple(event_type.value for event_type in cls)

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """Verifica se o valor informado representa um tipo válido."""

        if not isinstance(value, str):
            return False

        normalized = value.strip().lower()

        return normalized in cls.values()