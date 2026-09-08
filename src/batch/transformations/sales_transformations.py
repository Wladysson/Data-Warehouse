from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SalesTransformationConfig:
    event_type: str = "order"
    high_value_threshold: float = 10000.0

    def validate(self) -> None:
        if not self.event_type.strip():
            raise ValueError("event_type não pode ser vazio.")

        if self.high_value_threshold <= 0:
            raise ValueError(
                "high_value_threshold deve ser maior que zero."
            )


class SalesTransformations:

    def __init__(
        self,
        config: Optional[SalesTransformationConfig] = None,
    ) -> None:
        self.config = config or SalesTransformationConfig()
        self.config.validate()

    def filter_sales(self, dataframe: Any) -> Any:
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

    def normalize_status(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "status" not in dataframe.columns:
            logger.warning(
                "Coluna status não encontrada; "
                "normalização ignorada."
            )
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "status",
            F.lower(
                F.trim(
                    F.col("status")
                )
            ),
        )

    def cast_numeric_columns(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if "quantity" in result.columns:
            result = result.withColumn(
                "quantity",
                F.col("quantity").cast("integer"),
            )

        if "unit_price" in result.columns:
            result = result.withColumn(
                "unit_price",
                F.col("unit_price").cast("double"),
            )

        if "total_amount" in result.columns:
            result = result.withColumn(
                "total_amount",
                F.col("total_amount").cast("double"),
            )

        return result

    def calculate_total_amount(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        if (
            "quantity" not in dataframe.columns
            or "unit_price" not in dataframe.columns
        ):
            return dataframe

        result = dataframe

        calculated_total = F.round(
            F.col("quantity") * F.col("unit_price"),
            2,
        )

        if "total_amount" not in result.columns:
            return result.withColumn(
                "total_amount",
                calculated_total,
            )

        return result.withColumn(
            "calculated_total_amount",
            calculated_total,
        ).withColumn(
            "amount_difference",
            F.round(
                F.col("total_amount")
                - F.col("calculated_total_amount"),
                2,
            ),
        )

    def classify_order_value(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "total_amount" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        threshold = self.config.high_value_threshold

        return dataframe.withColumn(
            "order_value_category",
            F.when(
                F.col("total_amount") >= threshold,
                "high_value",
            )
            .when(
                F.col("total_amount") >= threshold * 0.5,
                "medium_value",
            )
            .otherwise("standard_value"),
        )

    def add_order_value_flags(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "total_amount" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        threshold = self.config.high_value_threshold

        result = dataframe.withColumn(
            "is_high_value_order",
            F.when(
                F.col("total_amount") >= threshold,
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "is_positive_sale",
            F.when(
                F.col("total_amount") > 0,
                1,
            ).otherwise(0),
        )

        return result

    def add_quantity_features(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "quantity" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe.withColumn(
            "large_quantity_order",
            F.when(
                F.col("quantity") >= 10,
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "multi_item_order",
            F.when(
                F.col("quantity") > 1,
                1,
            ).otherwise(0),
        )

        return result

    def add_order_status_flags(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "status" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe

        statuses = (
            "created",
            "confirmed",
            "processing",
            "shipped",
            "delivered",
            "cancelled",
        )

        for status in statuses:
            column_name = f"is_order_{status}"

            result = result.withColumn(
                column_name,
                F.when(
                    F.col("status") == status,
                    1,
                ).otherwise(0),
            )

        return result

    def add_temporal_features(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "event_timestamp" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe

        result = result.withColumn(
            "sale_hour",
            F.hour(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "sale_day_of_week",
            F.dayofweek(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "sale_day_of_month",
            F.dayofmonth(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "sale_month",
            F.month(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "sale_year",
            F.year(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "sale_is_weekend",
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

        result = self.filter_sales(dataframe)
        result = self.normalize_status(result)
        result = self.cast_numeric_columns(result)
        result = self.calculate_total_amount(result)
        result = self.classify_order_value(result)
        result = self.add_order_value_flags(result)
        result = self.add_quantity_features(result)
        result = self.add_order_status_flags(result)
        result = self.add_temporal_features(result)

        logger.info(
            "Transformações de pedidos e vendas concluídas."
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
            "status",
            "order_value_category",
        ):
            if column in transformed.columns:
                group_columns.append(column)

        if not group_columns:
            raise ValueError(
                "Nenhuma coluna válida para agrupamento foi encontrada."
            )

        aggregations = [
            F.count("*").alias("order_count"),
        ]

        if "quantity" in transformed.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_quantity"
                )
            )

        if "total_amount" in transformed.columns:
            aggregations.extend(
                [
                    F.round(
                        F.sum("total_amount"),
                        2,
                    ).alias("total_sales"),
                    F.round(
                        F.avg("total_amount"),
                        2,
                    ).alias("average_order_value"),
                    F.round(
                        F.max("total_amount"),
                        2,
                    ).alias("maximum_order_value"),
                ]
            )

        if "customer_id" in transformed.columns:
            aggregations.append(
                F.countDistinct(
                    "customer_id"
                ).alias("unique_customers")
            )

        return (
            transformed
            .groupBy(*group_columns)
            .agg(*aggregations)
        )

    def describe(self) -> dict[str, Any]:
        return {
            "event_type": self.config.event_type,
            "high_value_threshold": (
                self.config.high_value_threshold
            ),
            "transformation": "sales",
            "features": [
                "calculated_total_amount",
                "amount_difference",
                "order_value_category",
                "is_high_value_order",
                "is_positive_sale",
                "large_quantity_order",
                "multi_item_order",
                "is_order_created",
                "is_order_confirmed",
                "is_order_processing",
                "is_order_shipped",
                "is_order_delivered",
                "is_order_cancelled",
                "sale_hour",
                "sale_day_of_week",
                "sale_day_of_month",
                "sale_month",
                "sale_year",
                "sale_is_weekend",
            ],
        }