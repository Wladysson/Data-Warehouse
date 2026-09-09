from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class HiveTableDefinition:

    name: str
    database: str
    description: str
    columns: tuple[tuple[str, str], ...]
    partition_columns: tuple[tuple[str, str], ...] = ()
    format: str = "PARQUET"
    external: bool = True

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("O nome da tabela não pode ser vazio.")

        if not self.database.strip():
            raise ValueError("O banco da tabela não pode ser vazio.")

        if not self.columns:
            raise ValueError(
                "A tabela deve possuir pelo menos uma coluna."
            )

        if self.format.upper() not in {
            "PARQUET",
            "ORC",
            "TEXTFILE",
            "SEQUENCEFILE",
        }:
            raise ValueError(
                f"Formato Hive não suportado: {self.format}"
            )


class HiveTable:

    FACT_SALES: ClassVar[HiveTableDefinition] = HiveTableDefinition(
        name="fact_sales",
        database="ecommerce",
        description="Fatos comerciais de vendas e pedidos.",
        columns=(
            ("event_id", "STRING"),
            ("order_id", "STRING"),
            ("customer_id", "STRING"),
            ("product_id", "STRING"),
            ("event_timestamp", "TIMESTAMP"),
            ("quantity", "INT"),
            ("unit_price", "DECIMAL(18,2)"),
            ("total_amount", "DECIMAL(18,2)"),
            ("status", "STRING"),
            ("event_type", "STRING"),
        ),
        partition_columns=(
            ("event_date", "DATE"),
        ),
    )

    FACT_CLICKS: ClassVar[HiveTableDefinition] = HiveTableDefinition(
        name="fact_clicks",
        database="ecommerce",
        description="Fatos comportamentais de interação dos clientes.",
        columns=(
            ("event_id", "STRING"),
            ("customer_id", "STRING"),
            ("product_id", "STRING"),
            ("session_id", "STRING"),
            ("event_timestamp", "TIMESTAMP"),
            ("page", "STRING"),
            ("action", "STRING"),
            ("event_type", "STRING"),
        ),
        partition_columns=(
            ("event_date", "DATE"),
        ),
    )

    FACT_CARTS: ClassVar[HiveTableDefinition] = HiveTableDefinition(
        name="fact_carts",
        database="ecommerce",
        description="Fatos relacionados aos carrinhos de compras.",
        columns=(
            ("event_id", "STRING"),
            ("cart_id", "STRING"),
            ("customer_id", "STRING"),
            ("product_id", "STRING"),
            ("event_timestamp", "TIMESTAMP"),
            ("quantity", "INT"),
            ("action", "STRING"),
            ("event_type", "STRING"),
        ),
        partition_columns=(
            ("event_date", "DATE"),
        ),
    )

    FACT_DELIVERIES: ClassVar[HiveTableDefinition] = HiveTableDefinition(
        name="fact_deliveries",
        database="ecommerce",
        description="Fatos operacionais de entregas e logística.",
        columns=(
            ("event_id", "STRING"),
            ("order_id", "STRING"),
            ("delivery_id", "STRING"),
            ("customer_id", "STRING"),
            ("event_timestamp", "TIMESTAMP"),
            ("estimated_delivery", "TIMESTAMP"),
            ("actual_delivery", "TIMESTAMP"),
            ("status", "STRING"),
            ("carrier", "STRING"),
            ("event_type", "STRING"),
        ),
        partition_columns=(
            ("event_date", "DATE"),
        ),
    )

    DIM_CUSTOMER: ClassVar[HiveTableDefinition] = HiveTableDefinition(
        name="dim_customer",
        database="ecommerce",
        description="Dimensão de clientes.",
        columns=(
            ("customer_id", "STRING"),
            ("customer_name", "STRING"),
            ("customer_email", "STRING"),
            ("customer_segment", "STRING"),
            ("customer_status", "STRING"),
            ("created_at", "TIMESTAMP"),
            ("updated_at", "TIMESTAMP"),
        ),
    )

    DIM_PRODUCT: ClassVar[HiveTableDefinition] = HiveTableDefinition(
        name="dim_product",
        database="ecommerce",
        description="Dimensão de produtos.",
        columns=(
            ("product_id", "STRING"),
            ("product_name", "STRING"),
            ("category_id", "STRING"),
            ("category_name", "STRING"),
            ("brand", "STRING"),
            ("unit_price", "DECIMAL(18,2)"),
            ("product_status", "STRING"),
            ("created_at", "TIMESTAMP"),
            ("updated_at", "TIMESTAMP"),
        ),
    )

    DIM_CATEGORY: ClassVar[HiveTableDefinition] = HiveTableDefinition(
        name="dim_category",
        database="ecommerce",
        description="Dimensão de categorias de produtos.",
        columns=(
            ("category_id", "STRING"),
            ("category_name", "STRING"),
            ("parent_category_id", "STRING"),
            ("category_level", "INT"),
            ("category_status", "STRING"),
        ),
    )

    DIM_DATE: ClassVar[HiveTableDefinition] = HiveTableDefinition(
        name="dim_date",
        database="ecommerce",
        description="Dimensão calendário para análises temporais.",
        columns=(
            ("date_key", "INT"),
            ("full_date", "DATE"),
            ("day", "INT"),
            ("month", "INT"),
            ("month_name", "STRING"),
            ("quarter", "INT"),
            ("year", "INT"),
            ("week_of_year", "INT"),
            ("day_of_week", "INT"),
            ("day_name", "STRING"),
            ("is_weekend", "BOOLEAN"),
        ),
    )

    @classmethod
    def all(cls) -> tuple[HiveTableDefinition, ...]:

        return (
            cls.FACT_SALES,
            cls.FACT_CLICKS,
            cls.FACT_CARTS,
            cls.FACT_DELIVERIES,
            cls.DIM_CUSTOMER,
            cls.DIM_PRODUCT,
            cls.DIM_CATEGORY,
            cls.DIM_DATE,
        )

    @classmethod
    def facts(cls) -> tuple[HiveTableDefinition, ...]:

        return (
            cls.FACT_SALES,
            cls.FACT_CLICKS,
            cls.FACT_CARTS,
            cls.FACT_DELIVERIES,
        )

    @classmethod
    def dimensions(cls) -> tuple[HiveTableDefinition, ...]:

        return (
            cls.DIM_CUSTOMER,
            cls.DIM_PRODUCT,
            cls.DIM_CATEGORY,
            cls.DIM_DATE,
        )

    @classmethod
    def names(cls) -> tuple[str, ...]:

        return tuple(
            table.name
            for table in cls.all()
        )

    @classmethod
    def get(cls, name: str) -> HiveTableDefinition:

        normalized_name = name.strip().lower()

        for table in cls.all():
            if table.name == normalized_name:
                return table

        raise KeyError(
            f"Tabela Hive não encontrada: {name}"
        )

    @classmethod
    def create_statement(
        cls,
        table: HiveTableDefinition,
        *,
        if_not_exists: bool = True,
    ) -> str:

        prefix = "CREATE EXTERNAL TABLE" if table.external else "CREATE TABLE"

        if if_not_exists:
            prefix += " IF NOT EXISTS"

        qualified_name = (
            f"`{table.database}`.`{table.name}`"
        )

        columns = ",\n    ".join(
            f"`{name}` {data_type}"
            for name, data_type in table.columns
        )

        statement = (
            f"{prefix} {qualified_name} (\n"
            f"    {columns}\n"
            f")"
        )

        if table.partition_columns:
            partitions = ",\n    ".join(
                f"`{name}` {data_type}"
                for name, data_type in table.partition_columns
            )

            statement += (
                "\nPARTITIONED BY (\n"
                f"    {partitions}\n"
                ")"
            )

        statement += f"\nSTORED AS {table.format.upper()}"

        return statement

    @classmethod
    def describe(cls) -> dict[str, dict[str, object]]:

        return {
            table.name: {
                "database": table.database,
                "description": table.description,
                "columns": table.columns,
                "partition_columns": table.partition_columns,
                "format": table.format,
                "external": table.external,
            }
            for table in cls.all()
        }