from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from src.streaming.models import (
    AlertEvent,
    StreamingEvent,
    WindowAggregation,
)


@dataclass(frozen=True, slots=True)
class AlertRule(ABC):

    alert_type: str
    severity: str
    description: str

    @abstractmethod
    def evaluate(
        self,
        event: StreamingEvent,
        aggregation: Optional[WindowAggregation] = None,
    ) -> Optional[AlertEvent]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class HighSalesVolumeRule(AlertRule):

    threshold: int = 100

    def __init__(
        self,
        threshold: int = 100,
        severity: str = "WARNING",
    ) -> None:
        if threshold <= 0:
            raise ValueError(
                "threshold deve ser maior que zero."
            )

        object.__setattr__(
            self,
            "alert_type",
            "HIGH_SALES_VOLUME",
        )
        object.__setattr__(
            self,
            "severity",
            severity,
        )
        object.__setattr__(
            self,
            "description",
            "Volume de vendas acima do limite configurado.",
        )
        object.__setattr__(
            self,
            "threshold",
            threshold,
        )

    def evaluate(
        self,
        event: StreamingEvent,
        aggregation: Optional[WindowAggregation] = None,
    ) -> Optional[AlertEvent]:
        if aggregation is None:
            return None

        if aggregation.event_type not in {
            "order",
            "mixed",
        }:
            return None

        if aggregation.event_count < self.threshold:
            return None

        return AlertEvent(
            alert_id=(
                f"alert-sales-{aggregation.window_end.timestamp()}"
            ),
            alert_type=self.alert_type,
            detected_at=datetime.now(timezone.utc),
            event_timestamp=aggregation.window_end,
            severity=self.severity,
            message=(
                "Volume de vendas acima do limite: "
                f"{aggregation.event_count} eventos."
            ),
            event_type="order",
            metrics={
                "event_count": aggregation.event_count,
                "threshold": self.threshold,
                "window_start": (
                    aggregation.window_start.isoformat()
                ),
                "window_end": (
                    aggregation.window_end.isoformat()
                ),
            },
        )


@dataclass(frozen=True, slots=True)
class LargeOrderValueRule(AlertRule):

    threshold: Decimal = Decimal("10000.00")

    def __init__(
        self,
        threshold: Decimal = Decimal("10000.00"),
        severity: str = "HIGH",
    ) -> None:
        if threshold <= Decimal("0"):
            raise ValueError(
                "threshold deve ser maior que zero."
            )

        object.__setattr__(
            self,
            "alert_type",
            "LARGE_ORDER_VALUE",
        )
        object.__setattr__(
            self,
            "severity",
            severity,
        )
        object.__setattr__(
            self,
            "description",
            "Pedido individual acima do valor limite.",
        )
        object.__setattr__(
            self,
            "threshold",
            threshold,
        )

    def evaluate(
        self,
        event: StreamingEvent,
        aggregation: Optional[WindowAggregation] = None,
    ) -> Optional[AlertEvent]:
        if event.event_type != "order":
            return None

        if (
            event.total_amount is None
            or event.total_amount < self.threshold
        ):
            return None

        return AlertEvent(
            alert_id=f"alert-order-{event.event_id}",
            alert_type=self.alert_type,
            detected_at=datetime.now(timezone.utc),
            event_timestamp=event.event_timestamp,
            severity=self.severity,
            message=(
                "Pedido com valor acima do limite: "
                f"{event.total_amount}."
            ),
            event_type=event.event_type,
            customer_id=event.customer_id,
            product_id=event.product_id,
            metrics={
                "total_amount": str(
                    event.total_amount
                ),
                "threshold": str(
                    self.threshold
                ),
                "order_id": event.order_id,
            },
        )


@dataclass(frozen=True, slots=True)
class LateDeliveryRule(AlertRule):
    
    threshold_hours: int = 24

    def __init__(
        self,
        threshold_hours: int = 24,
        severity: str = "WARNING",
    ) -> None:
        if threshold_hours <= 0:
            raise ValueError(
                "threshold_hours deve ser maior que zero."
            )

        object.__setattr__(
            self,
            "alert_type",
            "LATE_DELIVERY",
        )
        object.__setattr__(
            self,
            "severity",
            severity,
        )
        object.__setattr__(
            self,
            "description",
            "Entrega ultrapassou o limite operacional.",
        )
        object.__setattr__(
            self,
            "threshold_hours",
            threshold_hours,
        )

    def evaluate(
        self,
        event: StreamingEvent,
        aggregation: Optional[WindowAggregation] = None,
    ) -> Optional[AlertEvent]:
        if event.event_type != "delivery":
            return None

        if event.status in {
            "delivered",
            "failed",
        }:
            return None

        delay_seconds = (
            event.ingestion_timestamp
            - event.event_timestamp
        ).total_seconds()

        threshold_seconds = (
            self.threshold_hours
            * 60
            * 60
        )

        if delay_seconds <= threshold_seconds:
            return None

        return AlertEvent(
            alert_id=f"alert-delivery-{event.event_id}",
            alert_type=self.alert_type,
            detected_at=datetime.now(timezone.utc),
            event_timestamp=event.event_timestamp,
            severity=self.severity,
            message=(
                "Entrega apresenta atraso superior a "
                f"{self.threshold_hours} horas."
            ),
            event_type=event.event_type,
            customer_id=event.customer_id,
            metrics={
                "delay_seconds": delay_seconds,
                "threshold_seconds": threshold_seconds,
                "order_id": event.order_id,
                "delivery_id": event.delivery_id,
                "carrier": event.metadata.get(
                    "carrier"
                ),
            },
        )