from datetime import datetime, timezone

from src.domain.events.base_event import BaseEvent
from src.domain.events.cart_event import CartEvent
from src.domain.events.click_event import ClickEvent
from src.domain.events.delivery_event import DeliveryEvent
from src.domain.events.order_event import OrderEvent


def test_base_event_creates_valid_event():
    event = BaseEvent(
        event_id="event-000001",
        event_type="CLICK",
        event_timestamp=datetime(2026, 9, 12, 22, 0, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 9, 12, 22, 0, 2, tzinfo=timezone.utc),
        customer_id="customer-000001",
    )

    assert event.event_id == "event-000001"
    assert event.event_type == "CLICK"
    assert event.customer_id == "customer-000001"


def test_click_event_contains_click_attributes():
    event = ClickEvent(
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
    assert event.session_id == "session-000001"
    assert event.page == "product-detail"


def test_cart_event_contains_cart_attributes():
    event = CartEvent(
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
    assert event.product_id == "product-000001"
    assert event.quantity == 2


def test_order_event_contains_order_attributes():
    event = OrderEvent(
        event_id="order-000001",
        event_type="ORDER",
        event_timestamp=datetime(2026, 9, 12, 22, 2, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 9, 12, 22, 2, 3, tzinfo=timezone.utc),
        customer_id="customer-000001",
        order_id="order-000001",
        product_id="product-000001",
        quantity=2,
        unit_price=149.90,
        total_amount=299.80,
        status="COMPLETED",
    )

    assert event.order_id == "order-000001"
    assert event.product_id == "product-000001"
    assert event.total_amount == 299.80


def test_delivery_event_contains_delivery_attributes():
    event = DeliveryEvent(
        event_id="delivery-000001",
        event_type="DELIVERY",
        event_timestamp=datetime(2026, 9, 13, 10, 0, tzinfo=timezone.utc),
        ingestion_timestamp=datetime(2026, 9, 13, 10, 0, 5, tzinfo=timezone.utc),
        customer_id="customer-000001",
        order_id="order-000001",
        delivery_id="delivery-000001",
        status="IN_TRANSIT",
        carrier="transportadora-demo",
    )

    assert event.order_id == "order-000001"
    assert event.delivery_id == "delivery-000001"
    assert event.carrier == "transportadora-demo"