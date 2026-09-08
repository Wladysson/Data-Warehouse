from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True, slots=True)
class ClickAggregationConfig:
    """Configuração das agregações de cliques."""

    event_type: str = "click"


class ClickAggregation:

    def __init__(
        self,
        config: ClickAggregationConfig | None = None,
    ) -> None:
        self.config = config or ClickAggregationConfig()

    @staticmethod
    def _validate_dataframe(dataframe: DataFrame) -> None:
        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                "clicks deve ser uma instância de pyspark.sql.DataFrame."
            )

    def prepare(self, clicks: DataFrame) -> DataFrame:

        self._validate_dataframe(clicks)

        if "event_type" not in clicks.columns:
            raise ValueError(
                "A coluna 'event_type' é obrigatória."
            )

        prepared = clicks.filter(
            F.col("event_type") == self.config.event_type
        )

        if "event_date" not in prepared.columns:
            prepared = prepared.withColumn(
                "event_date",
                F.to_date(F.col("event_timestamp")),
            )

        if "action" in prepared.columns:
            prepared = prepared.withColumn(
                "action",
                F.lower(F.trim(F.col("action"))),
            )

        if "page" in prepared.columns:
            prepared = prepared.withColumn(
                "page",
                F.lower(F.trim(F.col("page"))),
            )

        return prepared

    def aggregate_overall(
        self,
        clicks: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(clicks)

        aggregations = [
            F.count("*").alias("total_clicks"),
        ]

        if "customer_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
            )

        if "session_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("session_id").alias(
                    "unique_sessions"
                )
            )

        if "product_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("product_id").alias(
                    "unique_products"
                )
            )

        return prepared.agg(*aggregations)

    def aggregate_daily(
        self,
        clicks: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(clicks)

        aggregations = [
            F.count("*").alias("total_clicks"),
        ]

        if "customer_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
            )

        if "session_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("session_id").alias(
                    "unique_sessions"
                )
            )

        return (
            prepared.groupBy("event_date")
            .agg(*aggregations)
            .orderBy("event_date")
        )

    def aggregate_by_product(
        self,
        clicks: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(clicks)

        if "product_id" not in prepared.columns:
            raise ValueError(
                "A coluna 'product_id' é obrigatória para "
                "agregação por produto."
            )

        aggregations = [
            F.count("*").alias("click_count"),
        ]

        if "customer_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
            )

        if "session_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("session_id").alias(
                    "unique_sessions"
                )
            )

        return (
            prepared.filter(F.col("product_id").isNotNull())
            .groupBy("product_id")
            .agg(*aggregations)
            .orderBy(F.desc("click_count"))
        )

    def aggregate_by_page(
        self,
        clicks: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(clicks)

        if "page" not in prepared.columns:
            raise ValueError(
                "A coluna 'page' é obrigatória para "
                "agregação por página."
            )

        return (
            prepared.filter(F.col("page").isNotNull())
            .groupBy("page")
            .agg(
                F.count("*").alias("click_count"),
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
                if "customer_id" in prepared.columns
                else F.count("*").alias("unique_customers"),
            )
            .orderBy(F.desc("click_count"))
        )

    def aggregate_by_action(
        self,
        clicks: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(clicks)

        if "action" not in prepared.columns:
            raise ValueError(
                "A coluna 'action' é obrigatória para "
                "agregação por ação."
            )

        return (
            prepared.filter(F.col("action").isNotNull())
            .groupBy("action")
            .agg(
                F.count("*").alias("click_count"),
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
                if "customer_id" in prepared.columns
                else F.count("*").alias("unique_customers"),
            )
            .orderBy(F.desc("click_count"))
        )

    def aggregate_by_day_and_page(
        self,
        clicks: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(clicks)

        if "page" not in prepared.columns:
            raise ValueError(
                "A coluna 'page' é obrigatória para "
                "agregação temporal."
            )

        return (
            prepared.groupBy(
                "event_date",
                "page",
            )
            .agg(
                F.count("*").alias("click_count"),
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
                if "customer_id" in prepared.columns
                else F.count("*").alias("unique_customers"),
            )
            .orderBy(
                "event_date",
                F.desc("click_count"),
            )
        )

    def aggregate_by_customer(
        self,
        clicks: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(clicks)

        if "customer_id" not in prepared.columns:
            raise ValueError(
                "A coluna 'customer_id' é obrigatória para "
                "agregação por cliente."
            )

        aggregations = [
            F.count("*").alias("click_count"),
        ]

        if "session_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("session_id").alias(
                    "session_count"
                )
            )

        if "product_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("product_id").alias(
                    "products_interacted"
                )
            )

        if "page" in prepared.columns:
            aggregations.append(
                F.countDistinct("page").alias(
                    "pages_visited"
                )
            )

        return (
            prepared.filter(F.col("customer_id").isNotNull())
            .groupBy("customer_id")
            .agg(*aggregations)
            .orderBy(F.desc("click_count"))
        )

    def run(
        self,
        clicks: DataFrame,
    ) -> DataFrame:

        return self.aggregate_overall(clicks)

    def describe(self) -> dict[str, object]:
        """Retorna a configuração da agregação."""

        return {
            "operation": "click_aggregation",
            "event_type": self.config.event_type,
        }