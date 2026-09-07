from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from ..schemas import DeliveryEventSchema


DELIVERY_STATUSES = (
    "pending",
    "in_transit",
    "out_for_delivery",
    "delivered",
    "failed",
)

CARRIERS = (
    "carrier-alpha",
    "carrier-beta",
    "carrier-gamma",
    "carrier-delta",
)


def generate_delivery_event() -> DeliveryEventSchema:
    """
    Gera um evento relacionado ao processo de entrega.
    """

    event_timestamp = datetime.now(timezone.utc)

    status = DELIVERY_STATUSES[
        uuid4().int % len(DELIVERY_STATUSES)
    ]

    estimated_delivery = (
        event_timestamp + timedelta(
            days=(uuid4().int % 7) + 1
        )
    )

    actual_delivery = None

    if status == "delivered":
        actual_delivery = event_timestamp

    return DeliveryEventSchema(
        event_id=f"evt-{uuid4()}",
        event_type="delivery",
        event_timestamp=event_timestamp,
        ingestion_timestamp=datetime.now(timezone.utc),
        customer_id=f"customer-{uuid4().hex[:12]}",
        session_id=f"session-{uuid4().hex[:12]}",
        order_id=f"order-{uuid4().hex[:12]}",
        delivery_id=f"delivery-{uuid4().hex[:12]}",
        status=status,
        carrier=CARRIERS[
            uuid4().int % len(CARRIERS)
        ],
        estimated_delivery=estimated_delivery,
        actual_delivery=actual_delivery,
        metadata={
            "source": "logistics",
        },
    )