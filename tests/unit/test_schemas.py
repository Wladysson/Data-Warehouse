from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.data_generator.schemas import (
    CartEventSchema,
    ClickEventSchema,
    DeliveryEventSchema,
    EventSchema,
    OrderEventSchema,
)


def test_event_schema_accepts_valid_event():
    event = EventSchema(
        event_id="event-000001",
        event_type="CLICK",
        event_timestamp=datetime(2026, 9, 12, 22, 0, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 9, 12, 22, 0, 2, tzinfo=timezone.utc),
        customer_id="customer-000001",
        session_id="session-000001",
        metadata={"source": "test"},
    )

    assert event.event_id == "event-000001"
    assert event.event_type == "CLICK"
    assert event.customer_id == "customer-000001"


def test_event_schema_rejects_missing_required_fields():
    with pytest.raises(ValidationError):
        EventSchema(
            event_id="event-000001",
            event_type="CLICK",
        )


def test_click_schema_accepts_valid_event():
    event = ClickEventSchema(
        event_id="click-000001",
        event_type="CLICK",
        event_timestamp=datetime(2026, 9, 12, 22, 0, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 9, 12, 22, 0, 2, tzinfo=timezone.utc),
        customer_id="customer-000001",
        session_id="session-000001",
        product_id="product-000001",
        page="product-detail",
        action="view",
    )

    assert event.product_id == "product-000001"
    assert event.page == "product-detail"
    assert event.action == "view"


def test_cart_schema_accepts_valid_event():
    event = CartEventSchema(
        event_id="cart-000001",
        event_type="CART",
        event_timestamp=datetime(2026, 9, 12, 22, 1, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 9, 12, 22, 1, 2, tzinfo=timezone.utc),
        customer_id="customer-000001",
        session_id="session-000001",
        cart_id="cart-000001",
        product_id="product-000001",
        quantity=2,
        action="ADD",
    )

    assert event.cart_id == "cart-000001"
    assert event.quantity == 2
    assert event.action == "ADD"


def test_order_schema_accepts_valid_event():
    event = OrderEventSchema(
        event_id="order-000001",
        event_type="ORDER",
        event_timestamp=datetime(2026, 9, 12, 22, 2, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 9, 12, 22, 2, 3, tzinfo=timezone.utc),
        customer_id="customer-000001",
        session_id="session-000001",
        order_id="order-000001",
        product_id="product-000001",
        quantity=2,
        unit_price=149.90,
        total_amount=299.80,
        status="COMPLETED",
    )

    assert event.order_id == "order-000001"
    assert event.quantity == 2
    assert event.total_amount == 299.80


def test_delivery_schema_accepts_valid_event():
    event = DeliveryEventSchema(
        event_id="delivery-000001",
        event_type="DELIVERY",
        event_timestamp=datetime(2026, 9, 13, 10, 0, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 9, 13, 10, 0, 5, tzinfo=timezone.utc),
        customer_id="customer-000001",
        order_id="order-000001",
        delivery_id="delivery-000001",
        status="IN_TRANSIT",
        carrier="transportadora-demo",
        estimated_delivery=datetime(2026, 9, 15, 18, 0, tzinfo=timezone.utc),
    )

    assert event.delivery_id == "delivery-000001"
    assert event.status == "IN_TRANSIT"
    assert event.actual_delivery is None


def test_order_schema_rejects_invalid_quantity():
    with pytest.raises(ValidationError):
        OrderEventSchema(
            event_id="order-000001",
            event_type="ORDER",
            event_timestamp=datetime(2026, 9, 12, 22, 2, tzinfo=timezone.utc),
            ingestion_timestamp=datetime(2026, 9, 12, 22, 2, 3, tzinfo=timezone.utc),
            customer_id="customer-000001",
            order_id="order-000001",
            product_id="product-000001",
            quantity=0,
            unit_price=149.90,
            total_amount=299.80,
            status="COMPLETED",
        )