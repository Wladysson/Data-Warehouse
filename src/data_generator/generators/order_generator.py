from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from ..schemas import OrderEventSchema


ORDER_STATUSES = (
    "created",
    "confirmed",
    "processing",
)


def generate_order_event() -> OrderEventSchema:
    """
    Gera um evento de pedido/venda.
    """

    event_timestamp = datetime.now(timezone.utc)

    quantity = (uuid4().int % 5) + 1

    unit_price = (
        Decimal(uuid4().int % 99000 + 1000) / Decimal("100")
    )

    total_amount = (
        unit_price * Decimal(quantity)
    ).quantize(Decimal("0.01"))

    return OrderEventSchema(
        event_id=f"evt-{uuid4()}",
        event_type="order",
        event_timestamp=event_timestamp,
        ingestion_timestamp=datetime.now(timezone.utc),
        customer_id=f"customer-{uuid4().hex[:12]}",
        session_id=f"session-{uuid4().hex[:12]}",
        order_id=f"order-{uuid4().hex[:12]}",
        product_id=f"product-{uuid4().hex[:8]}",
        quantity=quantity,
        unit_price=unit_price.quantize(Decimal("0.01")),
        total_amount=total_amount,
        status=ORDER_STATUSES[
            uuid4().int % len(ORDER_STATUSES)
        ],
        metadata={
            "source": "checkout",
            "payment_method": "credit_card",
        },
    )