from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional


@dataclass(frozen=True, slots=True)
class StreamingEvent:

    event_id: str
    event_type: str
    event_timestamp: datetime
    ingestion_timestamp: datetime
    customer_id: str

    session_id: Optional[str] = None
    product_id: Optional[str] = None
    order_id: Optional[str] = None
    cart_id: Optional[str] = None
    delivery_id: Optional[str] = None

    quantity: Optional[int] = None
    unit_price: Optional[Decimal] = None
    total_amount: Optional[Decimal] = None

    status: Optional[str] = None
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )

    @property
    def event_time_epoch_millis(self) -> int:

        return int(
            self.event_timestamp.timestamp() * 1000
        )

    @property
    def ingestion_time_epoch_millis(self) -> int:

        return int(
            self.ingestion_timestamp.timestamp() * 1000
        )

    @property
    def is_late_candidate(self) -> bool:

        return (
            self.event_timestamp
            < self.ingestion_timestamp
        )

    def to_dict(self) -> Dict[str, Any]:

        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_timestamp": self.event_timestamp.isoformat(),
            "ingestion_timestamp": (
                self.ingestion_timestamp.isoformat()
            ),
            "customer_id": self.customer_id,
            "session_id": self.session_id,
            "product_id": self.product_id,
            "order_id": self.order_id,
            "cart_id": self.cart_id,
            "delivery_id": self.delivery_id,
            "quantity": self.quantity,
            "unit_price": (
                str(self.unit_price)
                if self.unit_price is not None
                else None
            ),
            "total_amount": (
                str(self.total_amount)
                if self.total_amount is not None
                else None
            ),
            "status": self.status,
            "metadata": self.metadata,
        }


@dataclass(frozen=True, slots=True)
class WindowAggregation:

    window_start: datetime
    window_end: datetime

    event_type: str
    event_count: int

    unique_customers: int = 0
    total_quantity: int = 0
    total_amount: Decimal = Decimal("0.00")

    key: Optional[str] = None
    metadata: Dict[str, Any] = field(
        default_factory=dict
    )

    @property
    def duration_seconds(self) -> float:

        return (
            self.window_end - self.window_start
        ).total_seconds()

    def to_dict(self) -> Dict[str, Any]:

        return {
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "event_type": self.event_type,
            "event_count": self.event_count,
            "unique_customers": self.unique_customers,
            "total_quantity": self.total_quantity,
            "total_amount": str(
                self.total_amount
            ),
            "key": self.key,
            "metadata": self.metadata,
        }


@dataclass(frozen=True, slots=True)
class AlertEvent:

    alert_id: str
    alert_type: str

    detected_at: datetime
    event_timestamp: datetime

    severity: str
    message: str

    event_type: Optional[str] = None
    customer_id: Optional[str] = None
    product_id: Optional[str] = None

    metrics: Dict[str, Any] = field(
        default_factory=dict
    )

    def to_dict(self) -> Dict[str, Any]:

        return {
            "alert_id": self.alert_id,
            "alert_type": self.alert_type,
            "detected_at": self.detected_at.isoformat(),
            "event_timestamp": (
                self.event_timestamp.isoformat()
            ),
            "severity": self.severity,
            "message": self.message,
            "event_type": self.event_type,
            "customer_id": self.customer_id,
            "product_id": self.product_id,
            "metrics": self.metrics,
        }