from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CartTransformationConfig:
    event_type: str = "cart"

    def validate(self) -> None:
        if not self.event_type.strip():
            raise ValueError("event_type não pode ser vazio.")


class CartTransformations:

    def __init__(
        self,
        config: Optional[CartTransformationConfig] = None,
    ) -> None:
        self.config = config or CartTransformationConfig()
        self.config.validate()

    def filter_carts(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "event_type" not in dataframe.columns:
            raise ValueError(
                "O DataFrame precisa possuir a coluna event_type."
            )

        from pyspark.sql import functions as F

        return dataframe.filter(
            F.col("event_type") == self.config.event_type
        )

    def normalize_action(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "action" not in dataframe.columns:
            logger.warning(
                "Coluna action não encontrada; normalização ignorada."
            )
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "action",
            F.lower(
                F.trim(
                    F.col("action")
                )
            ),
        )

    def classify_cart_action(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "action" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "cart_action_category",
            F.when(
                F.col("action") == "created",
                "creation",
            )
            .when(
                F.col("action") == "updated",
                "modification",
            )
            .when(
                F.col("action") == "removed",
                "removal",
            )
            .when(
                F.col("action") == "abandoned",
                "abandonment",
            )
            .otherwise("other"),
        )

    def add_quantity_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode be None.")

        if "quantity" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe.withColumn(
            "quantity",
            F.col("quantity").cast("integer"),
        )

        result = result.withColumn(
            "large_cart",
            F.when(
                F.col("quantity") >= 10,
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "single_item_cart",
            F.when(
                F.col("quantity") == 1,
                1,
            ).otherwise(0),
        )

        return result

    def add_cart_state_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "action" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe

        result = result.withColumn(
            "is_cart_created",
            F.when(
                F.col("action") == "created",
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "is_cart_updated",
            F.when(
                F.col("action") == "updated",
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "is_cart_removed",
            F.when(
                F.col("action") == "removed",
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "is_cart_abandoned",
            F.when(
                F.col("action") == "abandoned",
                1,
            ).otherwise(0),
        )

        return result

    def add_customer_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if "customer_id" in result.columns:
            result = result.withColumn(
                "has_customer",
                F.when(
                    F.col("customer_id").isNotNull()
                    & (
                        F.length(
                            F.trim(
                                F.col("customer_id")
                            )
                        ) > 0
                    ),
                    1,
                ).otherwise(0),
            )

        if "cart_id" in result.columns:
            result = result.withColumn(
                "has_cart",
                F.when(
                    F.col("cart_id").isNotNull()
                    & (
                        F.length(
                            F.trim(
                                F.col("cart_id")
                            )
                        ) > 0
                    ),
                    1,
                ).otherwise(0),
            )

        return result

    def add_temporal_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "event_timestamp" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe

        result = result.withColumn(
            "cart_hour",
            F.hour(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "cart_day_of_week",
            F.dayofweek(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "cart_is_weekend",
            F.when(
                F.dayofweek(
                    F.col("event_timestamp")
                ).isin(1, 7),
                1,
            ).otherwise(0),
        )

        return result

    def transform(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        result = self.filter_carts(dataframe)
        result = self.normalize_action(result)
        result = self.classify_cart_action(result)
        result = self.add_quantity_features(result)
        result = self.add_cart_state_features(result)
        result = self.add_customer_features(result)
        result = self.add_temporal_features(result)

        logger.info(
            "Transformações de eventos de carrinho concluídas."
        )

        return result

    def create_summary(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        transformed = self.transform(dataframe)

        from pyspark.sql import functions as F

        group_columns = []

        for column in (
            "event_date",
            "product_id",
            "action",
            "cart_action_category",
        ):
            if column in transformed.columns:
                group_columns.append(column)

        if not group_columns:
            raise ValueError(
                "Nenhuma coluna válida para agrupamento foi encontrada."
            )

        aggregations = [
            F.count("*").alias("cart_event_count"),
        ]

        if "quantity" in transformed.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_items"
                )
            )

        if "customer_id" in transformed.columns:
            aggregations.append(
                F.countDistinct(
                    "customer_id"
                ).alias("unique_customers")
            )

        if "cart_id" in transformed.columns:
            aggregations.append(
                F.countDistinct(
                    "cart_id"
                ).alias("unique_carts")
            )

        return (
            transformed
            .groupBy(*group_columns)
            .agg(*aggregations)
        )

    def describe(self) -> dict[str, Any]:
        return {
            "event_type": self.config.event_type,
            "transformation": "cart",
            "features": [
                "cart_action_category",
                "large_cart",
                "single_item_cart",
                "is_cart_created",
                "is_cart_updated",
                "is_cart_removed",
                "is_cart_abandoned",
                "has_customer",
                "has_cart",
                "cart_hour",
                "cart_day_of_week",
                "cart_is_weekend",
            ],
        }