"""
JOIN distribuído entre vendas e entregas no processamento batch.

Responsável por relacionar pedidos comerciais aos respectivos eventos
logísticos para análise integrada de vendas e desempenho de entrega.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


_ALLOWED_JOIN_TYPES: Final[frozenset[str]] = frozenset(
    {"inner", "left", "right", "full", "left_outer", "right_outer", "full_outer"}
)


@dataclass(frozen=True, slots=True)
class SalesDeliveryJoinConfig:

    sales_key: str = "order_id"
    delivery_key: str = "order_id"
    join_type: str = "left"

    def __post_init__(self) -> None:
        if not self.sales_key.strip():
            raise ValueError("sales_key não pode ser vazio.")

        if not self.delivery_key.strip():
            raise ValueError("delivery_key não pode ser vazio.")

        if self.join_type not in _ALLOWED_JOIN_TYPES:
            raise ValueError(
                f"Tipo de JOIN inválido: {self.join_type}. "
                f"Valores permitidos: {sorted(_ALLOWED_JOIN_TYPES)}."
            )


class SalesDeliveryJoin:

    def __init__(
        self,
        config: SalesDeliveryJoinConfig | None = None,
    ) -> None:
        self.config = config or SalesDeliveryJoinConfig()

    @staticmethod
    def _validate_dataframe(
        dataframe: DataFrame,
        name: str,
    ) -> None:
        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                f"{name} deve ser uma instância de pyspark.sql.DataFrame."
            )

    @staticmethod
    def _validate_key(
        dataframe: DataFrame,
        key: str,
        name: str,
    ) -> None:
        if key not in dataframe.columns:
            raise ValueError(
                f"Chave '{key}' não encontrada no DataFrame de {name}."
            )

    def prepare_delivery_columns(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:

        protected_columns = {
            self.config.delivery_key,
        }

        expressions = []

        for column in deliveries.columns:
            if column in protected_columns:
                expressions.append(F.col(column))
                continue

            if column.startswith("delivery_"):
                expressions.append(F.col(column))
            else:
                expressions.append(
                    F.col(column).alias(f"delivery_{column}")
                )

        return deliveries.select(*expressions)

    def join(
        self,
        sales: DataFrame,
        deliveries: DataFrame,
    ) -> DataFrame:

        self._validate_dataframe(sales, "sales")
        self._validate_dataframe(deliveries, "deliveries")

        self._validate_key(
            sales,
            self.config.sales_key,
            "sales",
        )

        self._validate_key(
            deliveries,
            self.config.delivery_key,
            "deliveries",
        )

        prepared_deliveries = self.prepare_delivery_columns(
            deliveries
        )

        return sales.join(
            prepared_deliveries,
            sales[self.config.sales_key]
            == prepared_deliveries[self.config.delivery_key],
            self.config.join_type,
        )

    def join_with_alias(
        self,
        sales: DataFrame,
        deliveries: DataFrame,
    ) -> DataFrame:

        self._validate_dataframe(sales, "sales")
        self._validate_dataframe(deliveries, "deliveries")

        self._validate_key(
            sales,
            self.config.sales_key,
            "sales",
        )

        self._validate_key(
            deliveries,
            self.config.delivery_key,
            "deliveries",
        )

        sales_alias = sales.alias("sales")
        delivery_alias = deliveries.alias("delivery")

        return sales_alias.join(
            delivery_alias,
            F.col(
                f"sales.{self.config.sales_key}"
            )
            == F.col(
                f"delivery.{self.config.delivery_key}"
            ),
            self.config.join_type,
        )

    def find_sales_without_delivery(
        self,
        sales: DataFrame,
        deliveries: DataFrame,
    ) -> DataFrame:

        self._validate_dataframe(sales, "sales")
        self._validate_dataframe(deliveries, "deliveries")

        self._validate_key(
            sales,
            self.config.sales_key,
            "sales",
        )

        self._validate_key(
            deliveries,
            self.config.delivery_key,
            "deliveries",
        )

        delivery_keys = deliveries.select(
            F.col(self.config.delivery_key).alias(
                "_delivery_order_key"
            )
        ).dropDuplicates()

        return sales.join(
            delivery_keys,
            sales[self.config.sales_key]
            == delivery_keys["_delivery_order_key"],
            "left_anti",
        )

    def find_deliveries_without_sale(
        self,
        sales: DataFrame,
        deliveries: DataFrame,
    ) -> DataFrame:
        
        self._validate_dataframe(sales, "sales")
        self._validate_dataframe(deliveries, "deliveries")

        self._validate_key(
            sales,
            self.config.sales_key,
            "sales",
        )

        self._validate_key(
            deliveries,
            self.config.delivery_key,
            "deliveries",
        )

        sales_keys = sales.select(
            F.col(self.config.sales_key).alias(
                "_sales_order_key"
            )
        ).dropDuplicates()

        return deliveries.join(
            sales_keys,
            deliveries[self.config.delivery_key]
            == sales_keys["_sales_order_key"],
            "left_anti",
        )

    def create_logistics_sales_summary(
        self,
        sales: DataFrame,
        deliveries: DataFrame,
    ) -> DataFrame:

        enriched = self.join(sales, deliveries)

        delivery_status_column = "delivery_status"

        if delivery_status_column not in enriched.columns:
            return enriched.groupBy().agg(
                F.count("*").alias("sales_count"),
                F.sum("total_amount").alias("total_sales_amount"),
            )

        return (
            enriched.groupBy(delivery_status_column)
            .agg(
                F.count("*").alias("sales_count"),
                F.sum("total_amount").alias("total_sales_amount"),
                F.avg("total_amount").alias("average_sale_amount"),
            )
            .orderBy(F.desc("sales_count"))
        )

    def create_delivery_performance_summary(
        self,
        sales: DataFrame,
        deliveries: DataFrame,
    ) -> DataFrame:

        enriched = self.join(sales, deliveries)

        expressions = [
            F.count("*").alias("total_sales"),
            F.sum("total_amount").alias("total_sales_amount"),
        ]

        if "delivery_late" in enriched.columns:
            expressions.append(
                F.sum(
                    F.when(
                        F.col("delivery_late") == True,
                        1,
                    ).otherwise(0)
                ).alias("late_delivery_count")
            )

        if "delivery_delay_hours" in enriched.columns:
            expressions.append(
                F.avg("delivery_delay_hours").alias(
                    "average_delivery_delay_hours"
                )
            )

        return enriched.agg(*expressions)

    def run(
        self,
        sales: DataFrame,
        deliveries: DataFrame,
    ) -> DataFrame:
        """Executa o fluxo principal do JOIN."""

        return self.join(sales, deliveries)

    def describe(self) -> dict[str, object]:
        """Retorna informações da configuração do JOIN."""

        return {
            "operation": "sales_delivery_join",
            "sales_key": self.config.sales_key,
            "delivery_key": self.config.delivery_key,
            "join_type": self.config.join_type,
        }