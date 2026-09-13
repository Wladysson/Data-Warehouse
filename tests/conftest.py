from datetime import datetime, timezone
from pathlib import Path

import pytest


@pytest.fixture
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture
def samples_dir(project_root: Path) -> Path:
    return project_root / "data" / "samples"


@pytest.fixture
def sample_event_timestamp() -> datetime:
    return datetime(2026, 9, 12, 22, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def sample_click_event() -> dict:
    return {
        "event_id": "click-000001",
        "event_type": "CLICK",
        "event_timestamp": "2026-09-12T22:00:00Z",
        "ingestion_timestamp": "2026-09-12T22:00:02Z",
        "customer_id": "customer-000001",
        "session_id": "session-000001",
        "product_id": "product-000001",
        "page": "product-detail",
        "action": "view",
    }


@pytest.fixture
def sample_cart_event() -> dict:
    return {
        "event_id": "cart-000001",
        "event_type": "CART",
        "event_timestamp": "2026-09-12T22:01:00Z",
        "ingestion_timestamp": "2026-09-12T22:01:02Z",
        "customer_id": "customer-000001",
        "session_id": "session-000001",
        "cart_id": "cart-000001",
        "product_id": "product-000001",
        "quantity": 2,
        "action": "ADD",
    }


@pytest.fixture
def sample_order_event() -> dict:
    return {
        "event_id": "order-000001",
        "event_type": "ORDER",
        "event_timestamp": "2026-09-12T22:02:00Z",
        "ingestion_timestamp": "2026-09-12T22:02:03Z",
        "customer_id": "customer-000001",
        "session_id": "session-000001",
        "order_id": "order-000001",
        "product_id": "product-000001",
        "quantity": 2,
        "unit_price": 149.90,
        "total_amount": 299.80,
        "status": "COMPLETED",
    }


@pytest.fixture
def sample_delivery_event() -> dict:
    return {
        "event_id": "delivery-000001",
        "event_type": "DELIVERY",
        "event_timestamp": "2026-09-13T10:00:00Z",
        "ingestion_timestamp": "2026-09-13T10:00:05Z",
        "customer_id": "customer-000001",
        "order_id": "order-000001",
        "delivery_id": "delivery-000001",
        "status": "IN_TRANSIT",
        "carrier": "transportadora-demo",
        "estimated_delivery": "2026-09-15T18:00:00Z",
        "actual_delivery": None,
    }