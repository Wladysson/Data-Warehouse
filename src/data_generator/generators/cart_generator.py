from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from ..schemas import CartEventSchema


CART_ACTIONS = (
    "created",
    "updated",
    "removed",
    "abandoned",
)


def generate_cart_event() -> CartEventSchema:
    """
    Gera um evento relacionado ao carrinho de compras.
    """

    event_timestamp = datetime.now(timezone.utc)

    return CartEventSchema(
        event_id=f"evt-{uuid4()}",
        event_type="cart",
        event_timestamp=event_timestamp,
        ingestion_timestamp=datetime.now(timezone.utc),
        customer_id=f"customer-{uuid4().hex[:12]}",
        session_id=f"session-{uuid4().hex[:12]}",
        cart_id=f"cart-{uuid4().hex[:12]}",
        product_id=f"product-{uuid4().hex[:8]}",
        quantity=(uuid4().int % 5) + 1,
        action=CART_ACTIONS[
            uuid4().int % len(CART_ACTIONS)
        ],
        metadata={
            "source": "web",
        },
    )