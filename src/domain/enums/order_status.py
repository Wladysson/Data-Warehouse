from __future__ import annotations

from enum import StrEnum


class OrderStatus(StrEnum):

    CREATED = "created"
    CONFIRMED = "confirmed"
    PROCESSING = "processing"
    COMPLETED = "completed"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"

    @classmethod
    def values(cls) -> tuple[str, ...]:

        return tuple(status.value for status in cls)

    @classmethod
    def is_valid(cls, value: str) -> bool:

        if not isinstance(value, str):
            return False

        normalized = value.strip().lower()

        return normalized in cls.values()

    @property
    def is_final(self) -> bool:

        return self in {
            self.COMPLETED,
            self.DELIVERED,
            self.CANCELLED,
        }

    @property
    def is_successful(self) -> bool:

        return self in {
            self.COMPLETED,
            self.DELIVERED,
        }

    @property
    def is_cancelled(self) -> bool:

        return self is self.CANCELLED