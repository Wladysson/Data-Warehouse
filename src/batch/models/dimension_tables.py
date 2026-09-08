from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class DimensionTableDefinition:

    name: str
    description: str
    columns: tuple[str, ...]
    business_key: str
    surrogate_key: str = ""
    partition_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("O nome da dimensão não pode ser vazio.")

        if not self.columns:
            raise ValueError("A dimensão deve possuir pelo menos uma coluna.")

        if not self.business_key.strip():
            raise ValueError("A chave de negócio da dimensão deve ser informada.")


@dataclass(frozen=True, slots=True)
class DimensionTable:

    CUSTOMER: ClassVar[DimensionTableDefinition] = DimensionTableDefinition(
        name="dim_customer",
        description="Dimensão de clientes da plataforma de e-commerce.",
        columns=(
            "customer_id",
            "customer_name",
            "customer_email",
            "customer_segment",
            "customer_status",
            "created_at",
            "updated_at",
        ),
        business_key="customer_id",
        surrogate_key="customer_sk",
    )

    PRODUCT: ClassVar[DimensionTableDefinition] = DimensionTableDefinition(
        name="dim_product",
        description="Dimensão de produtos comercializados na plataforma.",
        columns=(
            "product_id",
            "product_name",
            "category_id",
            "category_name",
            "brand",
            "unit_price",
            "product_status",
            "created_at",
            "updated_at",
        ),
        business_key="product_id",
        surrogate_key="product_sk",
    )

    CATEGORY: ClassVar[DimensionTableDefinition] = DimensionTableDefinition(
        name="dim_category",
        description="Dimensão de categorias utilizadas na classificação dos produtos.",
        columns=(
            "category_id",
            "category_name",
            "parent_category_id",
            "category_level",
            "category_status",
        ),
        business_key="category_id",
        surrogate_key="category_sk",
    )

    DATE: ClassVar[DimensionTableDefinition] = DimensionTableDefinition(
        name="dim_date",
        description="Dimensão temporal para análises diárias e sazonais.",
        columns=(
            "date_key",
            "full_date",
            "day",
            "month",
            "month_name",
            "quarter",
            "year",
            "week_of_year",
            "day_of_week",
            "day_name",
            "is_weekend",
        ),
        business_key="full_date",
        surrogate_key="date_key",
    )

    @classmethod
    def all(cls) -> tuple[DimensionTableDefinition, ...]:
        """Retorna todas as definições de dimensões."""

        return (
            cls.CUSTOMER,
            cls.PRODUCT,
            cls.CATEGORY,
            cls.DATE,
        )

    @classmethod
    def names(cls) -> tuple[str, ...]:
        """Retorna os nomes físicos das dimensões."""

        return tuple(table.name for table in cls.all())

    @classmethod
    def get(cls, name: str) -> DimensionTableDefinition:
        """Obtém uma dimensão pelo nome."""

        normalized_name = name.strip().lower()

        for table in cls.all():
            if table.name == normalized_name:
                return table

        raise KeyError(f"Dimensão não encontrada: {name}")

    @classmethod
    def describe(cls) -> dict[str, dict[str, object]]:

        return {
            table.name: {
                "description": table.description,
                "columns": table.columns,
                "business_key": table.business_key,
                "surrogate_key": table.surrogate_key,
                "partition_columns": table.partition_columns,
            }
            for table in cls.all()
        }