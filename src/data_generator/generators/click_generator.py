from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from ..schemas import ClickEventSchema


PAGES = (
    "home",
    "search",
    "product",
    "category",
    "checkout",
)

ACTIONS = (
    "view",
    "search",
    "product_view",
    "add_to_cart",
)


def generate_click_event() -> ClickEventSchema:
    """
    Gera um evento de clique/interação do cliente.
    """

    event_timestamp = datetime.now(timezone.utc)

    return ClickEventSchema(
        event_id=f"evt-{uuid4()}",
        event_type="click",
        event_timestamp=event_timestamp,
        ingestion_timestamp=datetime.now(timezone.utc),
        customer_id=f"customer-{uuid4().hex[:12]}",
        session_id=f"session-{uuid4().hex[:12]}",
        product_id=f"product-{uuid4().hex[:8]}",
        page=PAGES[
            uuid4().int % len(PAGES)
        ],
        action=ACTIONS[
            uuid4().int % len(ACTIONS)
        ],
        metadata={
            "source": "web",
            "device": "desktop",
        },
    )