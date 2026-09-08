from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True, slots=True)
class CartAggregationConfig:
    """Configuração das agregações de carrinhos."""

    event_type: str = "cart"


class CartAggregation:

    def __init__(
        self,
        config: CartAggregationConfig | None = None,
    ) -> None:
        self.config = config or CartAggregationConfig()

    @staticmethod
    def _validate_dataframe(dataframe: DataFrame) -> None:
        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                "carts deve ser uma instância de pyspark.sql.DataFrame."
            )

    def prepare(self, carts: DataFrame) -> DataFrame:
        """Prepara os eventos de carrinho para agregação."""

        self._validate_dataframe(carts)

        if "event_type" not in carts.columns:
            raise ValueError(
                "A coluna 'event_type' é obrigatória."
            )

        prepared = carts.filter(
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

        if "quantity" in prepared.columns:
            prepared = prepared.withColumn(
                "quantity",
                F.col("quantity").cast("long"),
            )

        return prepared

    def aggregate_overall(
        self,
        carts: DataFrame,
    ) -> DataFrame:
        """Calcula os principais indicadores gerais de carrinhos."""

        prepared = self.prepare(carts)

        aggregations = [
            F.count("*").alias("total_cart_events"),
        ]

        if "cart_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("cart_id").alias(
                    "unique_carts"
                )
            )

        if "customer_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
            )

        if "product_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("product_id").alias(
                    "unique_products"
                )
            )

        if "quantity" in prepared.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_items_in_cart_events"
                )
            )

        return prepared.agg(*aggregations)

    def aggregate_daily(
        self,
        carts: DataFrame,
    ) -> DataFrame:
        """Calcula métricas de carrinhos por dia."""

        prepared = self.prepare(carts)

        aggregations = [
            F.count("*").alias("cart_event_count"),
        ]

        if "cart_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("cart_id").alias(
                    "unique_carts"
                )
            )

        if "customer_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
            )

        if "quantity" in prepared.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_quantity"
                )
            )

        return (
            prepared.groupBy("event_date")
            .agg(*aggregations)
            .orderBy("event_date")
        )

    def aggregate_by_action(
        self,
        carts: DataFrame,
    ) -> DataFrame:
        """Consolida eventos de carrinho por ação."""

        prepared = self.prepare(carts)

        if "action" not in prepared.columns:
            raise ValueError(
                "A coluna 'action' é obrigatória para "
                "agregação por ação."
            )

        aggregations = [
            F.count("*").alias("event_count"),
        ]

        if "cart_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("cart_id").alias(
                    "unique_carts"
                )
            )

        if "customer_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
            )

        if "quantity" in prepared.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_quantity"
                )
            )

        return (
            prepared.filter(F.col("action").isNotNull())
            .groupBy("action")
            .agg(*aggregations)
            .orderBy(F.desc("event_count"))
        )

    def aggregate_by_product(
        self,
        carts: DataFrame,
    ) -> DataFrame:
        """Consolida atividade de carrinho por produto."""

        prepared = self.prepare(carts)

        if "product_id" not in prepared.columns:
            raise ValueError(
                "A coluna 'product_id' é obrigatória para "
                "agregação por produto."
            )

        aggregations = [
            F.count("*").alias("cart_event_count"),
        ]

        if "cart_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("cart_id").alias(
                    "unique_carts"
                )
            )

        if "customer_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
            )

        if "quantity" in prepared.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_quantity"
                )
            )

        return (
            prepared.filter(F.col("product_id").isNotNull())
            .groupBy("product_id")
            .agg(*aggregations)
            .orderBy(F.desc("cart_event_count"))
        )

    def aggregate_by_customer(
        self,
        carts: DataFrame,
    ) -> DataFrame:
        """Consolida atividade de carrinho por cliente."""

        prepared = self.prepare(carts)

        if "customer_id" not in prepared.columns:
            raise ValueError(
                "A coluna 'customer_id' é obrigatória para "
                "agregação por cliente."
            )

        aggregations = [
            F.count("*").alias("cart_event_count"),
        ]

        if "cart_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("cart_id").alias(
                    "cart_count"
                )
            )

        if "product_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("product_id").alias(
                    "products_interacted"
                )
            )

        if "quantity" in prepared.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_quantity"
                )
            )

        return (
            prepared.filter(F.col("customer_id").isNotNull())
            .groupBy("customer_id")
            .agg(*aggregations)
            .orderBy(F.desc("cart_event_count"))
        )

    def aggregate_daily_by_action(
        self,
        carts: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(carts)

        if "action" not in prepared.columns:
            raise ValueError(
                "A coluna 'action' é obrigatória para "
                "agregação temporal."
            )

        return (
            prepared.groupBy(
                "event_date",
                "action",
            )
            .agg(
                F.count("*").alias("event_count"),
                F.countDistinct("cart_id").alias(
                    "unique_carts"
                )
                if "cart_id" in prepared.columns
                else F.count("*").alias("unique_carts"),
                F.sum("quantity").alias("total_quantity")
                if "quantity" in prepared.columns
                else F.lit(0).alias("total_quantity"),
            )
            .orderBy(
                "event_date",
                "action",
            )
        )

    def calculate_abandonment_metrics(
        self,
        carts: DataFrame,
    ) -> DataFrame:
        
        prepared = self.prepare(carts)

        if "action" not in prepared.columns:
            raise ValueError(
                "A coluna 'action' é obrigatória para "
                "calcular métricas de abandono."
            )

        return prepared.agg(
            F.sum(
                F.when(
                    F.col("action").isin(
                        "add",
                        "add_to_cart",
                        "update",
                    ),
                    1,
                ).otherwise(0)
            ).alias("active_cart_events"),
            F.sum(
                F.when(
                    F.col("action").isin(
                        "checkout",
                        "purchase",
                        "converted",
                    ),
                    1,
                ).otherwise(0)
            ).alias("conversion_events"),
            F.sum(
                F.when(
                    F.col("action").isin(
                        "abandon",
                        "abandoned",
                    ),
                    1,
                ).otherwise(0)
            ).alias("abandonment_events"),
            F.sum(
                F.when(
                    F.col("action").isin(
                        "remove",
                        "removed",
                    ),
                    1,
                ).otherwise(0)
            ).alias("removal_events"),
        )

    def run(
        self,
        carts: DataFrame,
    ) -> DataFrame:

        return self.aggregate_overall(carts)

    def describe(self) -> dict[str, object]:
        """Retorna a configuração da agregação."""

        return {
            "operation": "cart_aggregation",
            "event_type": self.config.event_type,
        }