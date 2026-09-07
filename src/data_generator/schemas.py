from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


EventType = Literal[
    "click",
    "cart",
    "order",
    "delivery",
]

OrderStatus = Literal[
    "created",
    "confirmed",
    "processing",
    "shipped",
    "delivered",
    "cancelled",
]

DeliveryStatus = Literal[
    "pending",
    "in_transit",
    "out_for_delivery",
    "delivered",
    "failed",
]


class EventSchema(BaseModel):
    """
    Contrato base compartilhado por todos os eventos.
    """

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        str_strip_whitespace=True,
    )

    event_id: str = Field(
        min_length=1,
        max_length=100,
        description="Identificador único do evento.",
    )

    event_type: EventType = Field(
        description="Tipo funcional do evento.",
    )

    event_timestamp: datetime = Field(
        description="Timestamp de ocorrência do evento.",
    )

    ingestion_timestamp: datetime = Field(
        description="Timestamp de geração/ingestão do evento.",
    )

    customer_id: str = Field(
        min_length=1,
        max_length=100,
        description="Identificador do cliente.",
    )

    session_id: Optional[str] = Field(
        default=None,
        max_length=100,
        description="Identificador da sessão do cliente.",
    )

    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Metadados adicionais do evento.",
    )

    @field_validator("event_id", "customer_id", "session_id")
    @classmethod
    def validate_identifiers(
        cls,
        value: Optional[str],
    ) -> Optional[str]:
        if value is None:
            return None

        normalized = value.strip()

        if not normalized:
            raise ValueError("O identificador não pode ser vazio.")

        return normalized


class ClickEventSchema(EventSchema):
    """
    Evento de interação do cliente com um produto.
    """

    event_type: Literal["click"] = "click"

    product_id: str = Field(
        min_length=1,
        max_length=100,
        description="Identificador do produto acessado.",
    )

    page: str = Field(
        min_length=1,
        max_length=100,
        description="Página ou contexto da interação.",
    )

    action: Literal[
        "view",
        "search",
        "product_view",
        "add_to_cart",
    ] = Field(
        description="Ação realizada pelo cliente.",
    )


class CartEventSchema(EventSchema):
    """
    Evento relacionado ao carrinho de compras.
    """

    event_type: Literal["cart"] = "cart"

    cart_id: str = Field(
        min_length=1,
        max_length=100,
        description="Identificador do carrinho.",
    )

    product_id: str = Field(
        min_length=1,
        max_length=100,
        description="Identificador do produto.",
    )

    quantity: int = Field(
        ge=1,
        le=1000,
        description="Quantidade do produto no carrinho.",
    )

    action: Literal[
        "created",
        "updated",
        "removed",
        "abandoned",
    ] = Field(
        description="Operação realizada sobre o carrinho.",
    )


class OrderEventSchema(EventSchema):
    """
    Evento de pedido/venda realizado pelo cliente.
    """

    event_type: Literal["order"] = "order"

    order_id: str = Field(
        min_length=1,
        max_length=100,
        description="Identificador do pedido.",
    )

    product_id: str = Field(
        min_length=1,
        max_length=100,
        description="Identificador do produto.",
    )

    quantity: int = Field(
        ge=1,
        le=1000,
        description="Quantidade vendida.",
    )

    unit_price: Decimal = Field(
        ge=Decimal("0.01"),
        max_digits=12,
        decimal_places=2,
        description="Preço unitário do produto.",
    )

    total_amount: Decimal = Field(
        ge=Decimal("0.01"),
        max_digits=14,
        decimal_places=2,
        description="Valor total do item.",
    )

    status: OrderStatus = Field(
        description="Status atual do pedido.",
    )

    @field_validator("total_amount")
    @classmethod
    def validate_total_amount(
        cls,
        value: Decimal,
    ) -> Decimal:
        if value <= Decimal("0"):
            raise ValueError("O valor total deve ser positivo.")

        return value


class DeliveryEventSchema(EventSchema):
    """
    Evento relacionado ao processo logístico de entrega.
    """

    event_type: Literal["delivery"] = "delivery"

    order_id: str = Field(
        min_length=1,
        max_length=100,
        description="Identificador do pedido relacionado.",
    )

    delivery_id: str = Field(
        min_length=1,
        max_length=100,
        description="Identificador da entrega.",
    )

    status: DeliveryStatus = Field(
        description="Status atual da entrega.",
    )

    carrier: str = Field(
        min_length=1,
        max_length=100,
        description="Transportadora responsável.",
    )

    estimated_delivery: Optional[datetime] = Field(
        default=None,
        description="Previsão de entrega.",
    )

    actual_delivery: Optional[datetime] = Field(
        default=None,
        description="Data efetiva de entrega.",
    )