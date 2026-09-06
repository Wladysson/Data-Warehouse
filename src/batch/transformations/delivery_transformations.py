from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DeliveryTransformationConfig:
    event_type: str = "delivery"
    late_delivery_threshold_hours: int = 24

    def validate(self) -> None:
        if not self.event_type.strip():
            raise ValueError("event_type não pode ser vazio.")

        if self.late_delivery_threshold_hours <= 0:
            raise ValueError(
                "late_delivery_threshold_hours deve ser maior que zero."
            )


class DeliveryTransformations:

    def __init__(
        self,
        config: Optional[DeliveryTransformationConfig] = None,
    ) -> None:
        self.config = config or DeliveryTransformationConfig()
        self.config.validate()

    def filter_deliveries(self, dataframe: Any) -> Any:
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

    def normalize_carrier(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "carrier" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "carrier",
            F.trim(
                F.col("carrier")
            ),
        )

    def add_status_flags(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "status" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe

        statuses = (
            "pending",
            "in_transit",
            "out_for_delivery",
            "delivered",
            "failed",
        )

        for status in statuses:
            result = result.withColumn(
                f"is_delivery_{status}",
                F.when(
                    F.col("status") == status,
                    1,
                ).otherwise(0),
            )

        return result

    def classify_delivery_status(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "status" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "delivery_stage",
            F.when(
                F.col("status") == "pending",
                "awaiting_dispatch",
            )
            .when(
                F.col("status") == "in_transit",
                "transportation",
            )
            .when(
                F.col("status") == "out_for_delivery",
                "last_mile",
            )
            .when(
                F.col("status") == "delivered",
                "completed",
            )
            .when(
                F.col("status") == "failed",
                "exception",
            )
            .otherwise("unknown"),
        )

    def calculate_delivery_delay(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "event_timestamp" not in dataframe.columns:
            return dataframe

        if "estimated_delivery" not in dataframe.columns:
            logger.warning(
                "estimated_delivery não encontrada; "
                "atraso de entrega não será calculado."
            )
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe.withColumn(
            "delivery_delay_hours",
            (
                F.unix_timestamp(
                    F.col("event_timestamp")
                )
                - F.unix_timestamp(
                    F.col("estimated_delivery")
                )
            )
            / 3600.0,
        )

        threshold = self.config.late_delivery_threshold_hours

        result = result.withColumn(
            "is_late_delivery",
            F.when(
                F.col("delivery_delay_hours") > threshold,
                1,
            ).otherwise(0),
        )

        return result

    def calculate_delivery_completion_time(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        required_columns = {
            "event_timestamp",
            "actual_delivery",
        }

        if not required_columns.issubset(
            set(dataframe.columns)
        ):
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "delivery_completion_hours",
            (
                F.unix_timestamp(
                    F.col("actual_delivery")
                )
                - F.unix_timestamp(
                    F.col("event_timestamp")
                )
            )
            / 3600.0,
        )

    def add_temporal_features(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "event_timestamp" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe

        result = result.withColumn(
            "delivery_hour",
            F.hour(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "delivery_day_of_week",
            F.dayofweek(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "delivery_month",
            F.month(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "delivery_year",
            F.year(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "delivery_is_weekend",
            F.when(
                F.dayofweek(
                    F.col("event_timestamp")
                ).isin(1, 7),
                1,
            ).otherwise(0),
        )

        return result

    def add_logistics_quality_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if "status" in result.columns:
            result = result.withColumn(
                "delivery_success",
                F.when(
                    F.col("status") == "delivered",
                    1,
                ).otherwise(0),
            )

            result = result.withColumn(
                "delivery_exception",
                F.when(
                    F.col("status") == "failed",
                    1,
                ).otherwise(0),
            )

        if "delivery_delay_hours" in result.columns:
            result = result.withColumn(
                "delivery_delay_category",
                F.when(
                    F.col("delivery_delay_hours") <= 0,
                    "on_time",
                )
                .when(
                    F.col("delivery_delay_hours") <= 24,
                    "minor_delay",
                )
                .when(
                    F.col("delivery_delay_hours") <= 72,
                    "moderate_delay",
                )
                .otherwise("severe_delay"),
            )

        return result

    def transform(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        result = self.filter_deliveries(dataframe)
        result = self.normalize_status(result)
        result = self.normalize_carrier(result)
        result = self.add_status_flags(result)
        result = self.classify_delivery_status(result)
        result = self.calculate_delivery_delay(result)
        result = self.calculate_delivery_completion_time(result)
        result = self.add_temporal_features(result)
        result = self.add_logistics_quality_features(result)

        logger.info(
            "Transformações de eventos de entrega concluídas."
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
            "carrier",
            "status",
            "delivery_stage",
            "delivery_delay_category",
        ):
            if column in transformed.columns:
                group_columns.append(column)

        if not group_columns:
            raise ValueError(
                "Nenhuma coluna válida para agrupamento foi encontrada."
            )

        aggregations = [
            F.count("*").alias("delivery_event_count"),
        ]

        if "order_id" in transformed.columns:
            aggregations.append(
                F.countDistinct(
                    "order_id"
                ).alias("unique_orders")
            )

        if "customer_id" in transformed.columns:
            aggregations.append(
                F.countDistinct(
                    "customer_id"
                ).alias("unique_customers")
            )

        if "delivery_delay_hours" in transformed.columns:
            aggregations.append(
                F.round(
                    F.avg(
                        "delivery_delay_hours"
                    ),
                    2,
                ).alias("average_delay_hours")
            )

        return (
            transformed
            .groupBy(*group_columns)
            .agg(*aggregations)
        )

    def describe(self) -> dict[str, Any]:
        return {
            "event_type": self.config.event_type,
            "late_delivery_threshold_hours": (
                self.config.late_delivery_threshold_hours
            ),
            "transformation": "delivery",
            "features": [
                "is_delivery_pending",
                "is_delivery_in_transit",
                "is_delivery_out_for_delivery",
                "is_delivery_delivered",
                "is_delivery_failed",
                "delivery_stage",
                "delivery_delay_hours",
                "is_late_delivery",
                "delivery_completion_hours",
                "delivery_hour",
                "delivery_day_of_week",
                "delivery_month",
                "delivery_year",
                "delivery_is_weekend",
                "delivery_success",
                "delivery_exception",
                "delivery_delay_category",
            ],
        }