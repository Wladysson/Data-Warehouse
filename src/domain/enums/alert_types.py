from __future__ import annotations

from enum import StrEnum


class AlertType(StrEnum):

    HIGH_SALES_VOLUME = "high_sales_volume"
    LARGE_ORDER_VALUE = "large_order_value"
    LATE_DELIVERY = "late_delivery"
    DELIVERY_FAILURE = "delivery_failure"
    CART_ABANDONMENT = "cart_abandonment"
    EVENT_LAG = "event_lag"
    OUT_OF_ORDER_EVENT = "out_of_order_event"
    PIPELINE_HEALTH = "pipeline_health"

    @classmethod
    def values(cls) -> tuple[str, ...]:

        return tuple(alert_type.value for alert_type in cls)

    @classmethod
    def is_valid(cls, value: str) -> bool:

        if not isinstance(value, str):
            return False

        normalized = value.strip().lower()

        return normalized in cls.values()

    @property
    def is_commercial(self) -> bool:

        return self in {
            self.HIGH_SALES_VOLUME,
            self.LARGE_ORDER_VALUE,
        }

    @property
    def is_logistics(self) -> bool:

        return self in {
            self.LATE_DELIVERY,
            self.DELIVERY_FAILURE,
        }

    @property
    def is_behavioral(self) -> bool:

        return self is self.CART_ABANDONMENT

    @property
    def is_pipeline(self) -> bool:

        return self in {
            self.EVENT_LAG,
            self.OUT_OF_ORDER_EVENT,
            self.PIPELINE_HEALTH,
        }