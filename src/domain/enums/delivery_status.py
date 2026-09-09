from __future__ import annotations

from enum import StrEnum


class DeliveryStatus(StrEnum):

    PENDING = "pending"
    PREPARING = "preparing"
    SHIPPED = "shipped"
    IN_TRANSIT = "in_transit"
    OUT_FOR_DELIVERY = "out_for_delivery"
    DELIVERED = "delivered"
    DELAYED = "delayed"
    FAILED = "failed"
    RETURNED = "returned"
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
            self.DELIVERED,
            self.FAILED,
            self.RETURNED,
            self.CANCELLED,
        }

    @property
    def is_successful(self) -> bool:

        return self is self.DELIVERED

    @property
    def is_problematic(self) -> bool:

        return self in {
            self.DELAYED,
            self.FAILED,
            self.RETURNED,
            self.CANCELLED,
        }

    @property
    def is_in_transit(self) -> bool:

        return self in {
            self.SHIPPED,
            self.IN_TRANSIT,
            self.OUT_FOR_DELIVERY,
        }