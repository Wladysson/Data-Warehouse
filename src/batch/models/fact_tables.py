from dataclasses import dataclass, field
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class FactTableDefinition:

    name: str
    description: str
    columns: tuple[str, ...]
    partition_columns: tuple[str, ...] = ()
    primary_grain: str = ""

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("O nome da tabela fato não pode ser vazio.")

        if not self.columns:
            raise ValueError("A tabela fato deve possuir pelo menos uma coluna.")

        if not self.primary_grain.strip():
            raise ValueError("O grão da tabela fato deve ser informado.")


@dataclass(frozen=True, slots=True)
class FactTable:

    SALES: ClassVar[FactTableDefinition] = FactTableDefinition(
        name="fact_sales",
        description="Fatos comerciais relacionados aos pedidos e vendas.",
        columns=(
            "event_id",
            "order_id",
            "customer_id",
            "product_id",
            "event_timestamp",
            "quantity",
            "unit_price",
            "total_amount",
            "status",
            "event_type",
            "event_date",
        ),
        partition_columns=("event_date",),
        primary_grain="um registro por evento de venda/pedido",
    )

    CLICKS: ClassVar[FactTableDefinition] = FactTableDefinition(
        name="fact_clicks",
        description="Fatos comportamentais relacionados às interações dos clientes.",
        columns=(
            "event_id",
            "customer_id",
            "product_id",
            "session_id",
            "event_timestamp",
            "page",
            "action",
            "event_type",
            "event_date",
        ),
        partition_columns=("event_date",),
        primary_grain="um registro por interação de clique",
    )

    CARTS: ClassVar[FactTableDefinition] = FactTableDefinition(
        name="fact_carts",
        description="Fatos relacionados às operações realizadas nos carrinhos.",
        columns=(
            "event_id",
            "cart_id",
            "customer_id",
            "product_id",
            "event_timestamp",
            "quantity",
            "action",
            "event_type",
            "event_date",
        ),
        partition_columns=("event_date",),
        primary_grain="um registro por evento de carrinho",
    )

    DELIVERIES: ClassVar[FactTableDefinition] = FactTableDefinition(
        name="fact_deliveries",
        description="Fatos operacionais relacionados à logística e entregas.",
        columns=(
            "event_id",
            "order_id",
            "delivery_id",
            "customer_id",
            "event_timestamp",
            "estimated_delivery",
            "actual_delivery",
            "status",
            "carrier",
            "event_type",
            "event_date",
        ),
        partition_columns=("event_date",),
        primary_grain="um registro por evento de entrega",
    )

    @classmethod
    def all(cls) -> tuple[FactTableDefinition, ...]:

        return (
            cls.SALES,
            cls.CLICKS,
            cls.CARTS,
            cls.DELIVERIES,
        )

    @classmethod
    def names(cls) -> tuple[str, ...]:
        """Retorna os nomes físicos das tabelas fato."""

        return tuple(table.name for table in cls.all())

    @classmethod
    def get(cls, name: str) -> FactTableDefinition:
        """Obtém uma tabela fato pelo nome."""

        normalized_name = name.strip().lower()

        for table in cls.all():
            if table.name == normalized_name:
                return table

        raise KeyError(f"Tabela fato não encontrada: {name}")

    @classmethod
    def describe(cls) -> dict[str, dict[str, object]]:

        return {
            table.name: {
                "description": table.description,
                "columns": table.columns,
                "partition_columns": table.partition_columns,
                "primary_grain": table.primary_grain,
            }
            for table in cls.all()
        }