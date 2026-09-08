from __future__ import annotations

from uuid import uuid4


def _generate_id(prefix: str) -> str:
    """
    Gera um identificador UUID com prefixo.
    """

    if not prefix or not prefix.strip():
        raise ValueError(
            "O prefixo do identificador não pode ser vazio."
        )

    return f"{prefix.strip()}-{uuid4().hex}"


def generate_event_id() -> str:
    """Gera identificador único de evento."""
    return _generate_id("evt")


def generate_customer_id() -> str:
    """Gera identificador de cliente."""
    return _generate_id("customer")


def generate_session_id() -> str:
    """Gera identificador de sessão."""
    return _generate_id("session")


def generate_product_id() -> str:
    """Gera identificador de produto."""
    return _generate_id("product")


def generate_cart_id() -> str:
    """Gera identificador de carrinho."""
    return _generate_id("cart")


def generate_order_id() -> str:
    """Gera identificador de pedido."""
    return _generate_id("order")


def generate_delivery_id() -> str:
    """Gera identificador de entrega."""
    return _generate_id("delivery")