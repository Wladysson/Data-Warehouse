"""
Agregações distribuídas de vendas utilizando Apache Spark.

Responsável pela consolidação de métricas comerciais, pedidos,
receita, ticket médio e desempenho das vendas.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True, slots=True)
class SalesAggregationConfig:

    event_type: str = "order"
    completed_statuses: tuple[str, ...] = (
        "confirmed",
        "processing",
        "completed",
        "delivered",
    )
    high_value_threshold: float = 10_000.0


class SalesAggregation:

    def __init__(
        self,
        config: SalesAggregationConfig | None = None,
    ) -> None:
        self.config = config or SalesAggregationConfig()

    @staticmethod
    def _validate_dataframe(dataframe: DataFrame) -> None:
        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                "sales deve ser uma instância de pyspark.sql.DataFrame."
            )

    def prepare(self, sales: DataFrame) -> DataFrame:
        """Prepara os registros de vendas para agregação."""

        self._validate_dataframe(sales)

        required_columns = {
            "event_type",
            "total_amount",
        }

        missing_columns = required_columns.difference(sales.columns)

        if missing_columns:
            raise ValueError(
                "Colunas obrigatórias ausentes no DataFrame de vendas: "
                f"{sorted(missing_columns)}."
            )

        prepared = (
            sales.filter(
                F.col("event_type") == self.config.event_type
            )
            .withColumn(
                "total_amount",
                F.col("total_amount").cast("double"),
            )
        )

        if "quantity" in prepared.columns:
            prepared = prepared.withColumn(
                "quantity",
                F.col("quantity").cast("long"),
            )

        if "status" in prepared.columns:
            prepared = prepared.withColumn(
                "status",
                F.lower(F.trim(F.col("status"))),
            )

        if "event_date" not in prepared.columns:
            prepared = prepared.withColumn(
                "event_date",
                F.to_date(F.col("event_timestamp")),
            )

        return prepared

    def aggregate_overall(
        self,
        sales: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(sales)

        aggregations = [
            F.count("*").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("average_order_value"),
            F.min("total_amount").alias("minimum_order_value"),
            F.max("total_amount").alias("maximum_order_value"),
        ]

        if "quantity" in prepared.columns:
            aggregations.append(
                F.sum("quantity").alias("total_items_sold")
            )

        return prepared.agg(*aggregations)

    def aggregate_daily(
        self,
        sales: DataFrame,
    ) -> DataFrame:
        """Calcula métricas comerciais por dia."""

        prepared = self.prepare(sales)

        aggregations = [
            F.count("*").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("average_order_value"),
            F.min("total_amount").alias("minimum_order_value"),
            F.max("total_amount").alias("maximum_order_value"),
        ]

        if "quantity" in prepared.columns:
            aggregations.append(
                F.sum("quantity").alias("total_items_sold")
            )

        return (
            prepared.groupBy("event_date")
            .agg(*aggregations)
            .orderBy("event_date")
        )

    def aggregate_by_status(
        self,
        sales: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(sales)

        if "status" not in prepared.columns:
            raise ValueError(
                "A coluna 'status' é obrigatória para agregação por status."
            )

        return (
            prepared.groupBy("status")
            .agg(
                F.count("*").alias("order_count"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("average_order_value"),
            )
            .orderBy(F.desc("order_count"))
        )

    def aggregate_by_customer(
        self,
        sales: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(sales)

        if "customer_id" not in prepared.columns:
            raise ValueError(
                "A coluna 'customer_id' é obrigatória para "
                "agregação por cliente."
            )

        return (
            prepared.filter(F.col("customer_id").isNotNull())
            .groupBy("customer_id")
            .agg(
                F.count("*").alias("order_count"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("average_order_value"),
                F.max("total_amount").alias("highest_order_value"),
            )
            .withColumn(
                "high_value_customer",
                F.col("total_revenue")
                >= self.config.high_value_threshold,
            )
            .orderBy(F.desc("total_revenue"))
        )

    def aggregate_by_product(
        self,
        sales: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(sales)

        if "product_id" not in prepared.columns:
            raise ValueError(
                "A coluna 'product_id' é obrigatória para "
                "agregação por produto."
            )

        aggregations = [
            F.count("*").alias("order_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("average_order_value"),
        ]

        if "quantity" in prepared.columns:
            aggregations.append(
                F.sum("quantity").alias("total_quantity_sold")
            )

        return (
            prepared.filter(F.col("product_id").isNotNull())
            .groupBy("product_id")
            .agg(*aggregations)
            .orderBy(F.desc("total_revenue"))
        )

    def aggregate_high_value_orders(
        self,
        sales: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(sales)

        return prepared.agg(
            F.sum(
                F.when(
                    F.col("total_amount")
                    >= self.config.high_value_threshold,
                    1,
                ).otherwise(0)
            ).alias("high_value_order_count"),
            F.sum(
                F.when(
                    F.col("total_amount")
                    >= self.config.high_value_threshold,
                    F.col("total_amount"),
                ).otherwise(F.lit(0.0))
            ).alias("high_value_revenue"),
        )

    def aggregate_by_day_and_status(
        self,
        sales: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(sales)

        if "status" not in prepared.columns:
            raise ValueError(
                "A coluna 'status' é obrigatória para "
                "agregação por dia e status."
            )

        return (
            prepared.groupBy(
                "event_date",
                "status",
            )
            .agg(
                F.count("*").alias("order_count"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("average_order_value"),
            )
            .orderBy(
                "event_date",
                "status",
            )
        )

    def run(
        self,
        sales: DataFrame,
    ) -> DataFrame:
        """Executa a agregação principal de vendas."""

        return self.aggregate_overall(sales)

    def describe(self) -> dict[str, object]:

        return {
            "operation": "sales_aggregation",
            "event_type": self.config.event_type,
            "completed_statuses": self.config.completed_statuses,
            "high_value_threshold": self.config.high_value_threshold,
        }