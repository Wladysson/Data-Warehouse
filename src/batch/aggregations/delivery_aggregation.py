from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True, slots=True)
class DeliveryAggregationConfig:
    """Configuração das agregações de entregas."""

    event_type: str = "delivery"
    late_threshold_hours: float = 24.0


class DeliveryAggregation:

    def __init__(
        self,
        config: DeliveryAggregationConfig | None = None,
    ) -> None:
        self.config = config or DeliveryAggregationConfig()

    @staticmethod
    def _validate_dataframe(dataframe: DataFrame) -> None:
        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                "deliveries deve ser uma instância de "
                "pyspark.sql.DataFrame."
            )

    def prepare(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:
        """Prepara os eventos de entrega para agregação."""

        self._validate_dataframe(deliveries)

        if "event_type" not in deliveries.columns:
            raise ValueError(
                "A coluna 'event_type' é obrigatória."
            )

        prepared = deliveries.filter(
            F.col("event_type") == self.config.event_type
        )

        if "event_date" not in prepared.columns:
            prepared = prepared.withColumn(
                "event_date",
                F.to_date(F.col("event_timestamp")),
            )

        if "status" in prepared.columns:
            prepared = prepared.withColumn(
                "status",
                F.lower(F.trim(F.col("status"))),
            )

        if "carrier" in prepared.columns:
            prepared = prepared.withColumn(
                "carrier",
                F.trim(F.col("carrier")),
            )

        if "delivery_delay_hours" in prepared.columns:
            prepared = prepared.withColumn(
                "delivery_delay_hours",
                F.col("delivery_delay_hours").cast("double"),
            )

        if "delivery_completion_hours" in prepared.columns:
            prepared = prepared.withColumn(
                "delivery_completion_hours",
                F.col("delivery_completion_hours").cast("double"),
            )

        return prepared

    def aggregate_overall(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(deliveries)

        aggregations = [
            F.count("*").alias("total_delivery_events"),
        ]

        if "delivery_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("delivery_id").alias(
                    "unique_deliveries"
                )
            )

        if "order_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("order_id").alias(
                    "unique_orders"
                )
            )

        if "delivery_delay_hours" in prepared.columns:
            aggregations.extend(
                [
                    F.avg("delivery_delay_hours").alias(
                        "average_delay_hours"
                    ),
                    F.max("delivery_delay_hours").alias(
                        "maximum_delay_hours"
                    ),
                ]
            )

        return prepared.agg(*aggregations)

    def aggregate_daily(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(deliveries)

        aggregations = [
            F.count("*").alias("delivery_event_count"),
        ]

        if "delivery_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("delivery_id").alias(
                    "unique_deliveries"
                )
            )

        if "order_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("order_id").alias(
                    "unique_orders"
                )
            )

        if "delivery_delay_hours" in prepared.columns:
            aggregations.append(
                F.avg("delivery_delay_hours").alias(
                    "average_delay_hours"
                )
            )

        return (
            prepared.groupBy("event_date")
            .agg(*aggregations)
            .orderBy("event_date")
        )

    def aggregate_by_status(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:
        """Consolida entregas por status."""

        prepared = self.prepare(deliveries)

        if "status" not in prepared.columns:
            raise ValueError(
                "A coluna 'status' é obrigatória para "
                "agregação por status."
            )

        aggregations = [
            F.count("*").alias("delivery_event_count"),
        ]

        if "delivery_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("delivery_id").alias(
                    "unique_deliveries"
                )
            )

        if "order_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("order_id").alias(
                    "unique_orders"
                )
            )

        if "delivery_delay_hours" in prepared.columns:
            aggregations.append(
                F.avg("delivery_delay_hours").alias(
                    "average_delay_hours"
                )
            )

        return (
            prepared.filter(F.col("status").isNotNull())
            .groupBy("status")
            .agg(*aggregations)
            .orderBy(F.desc("delivery_event_count"))
        )

    def aggregate_by_carrier(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:
        """Consolida indicadores por transportadora."""

        prepared = self.prepare(deliveries)

        if "carrier" not in prepared.columns:
            raise ValueError(
                "A coluna 'carrier' é obrigatória para "
                "agregação por transportadora."
            )

        aggregations = [
            F.count("*").alias("delivery_event_count"),
        ]

        if "delivery_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("delivery_id").alias(
                    "unique_deliveries"
                )
            )

        if "order_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("order_id").alias(
                    "unique_orders"
                )
            )

        if "delivery_delay_hours" in prepared.columns:
            aggregations.extend(
                [
                    F.avg("delivery_delay_hours").alias(
                        "average_delay_hours"
                    ),
                    F.max("delivery_delay_hours").alias(
                        "maximum_delay_hours"
                    ),
                ]
            )

        return (
            prepared.filter(F.col("carrier").isNotNull())
            .groupBy("carrier")
            .agg(*aggregations)
            .orderBy(F.desc("delivery_event_count"))
        )

    def aggregate_by_order(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(deliveries)

        if "order_id" not in prepared.columns:
            raise ValueError(
                "A coluna 'order_id' é obrigatória para "
                "agregação por pedido."
            )

        aggregations = [
            F.count("*").alias("delivery_event_count"),
        ]

        if "delivery_id" in prepared.columns:
            aggregations.append(
                F.countDistinct("delivery_id").alias(
                    "delivery_count"
                )
            )

        if "delivery_delay_hours" in prepared.columns:
            aggregations.append(
                F.max("delivery_delay_hours").alias(
                    "maximum_delay_hours"
                )
            )

        return (
            prepared.filter(F.col("order_id").isNotNull())
            .groupBy("order_id")
            .agg(*aggregations)
            .orderBy(F.desc("delivery_event_count"))
        )

    def calculate_delay_metrics(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:
        """Calcula indicadores globais de atraso logístico."""

        prepared = self.prepare(deliveries)

        if "delivery_delay_hours" not in prepared.columns:
            raise ValueError(
                "A coluna 'delivery_delay_hours' é obrigatória "
                "para calcular métricas de atraso."
            )

        return prepared.agg(
            F.count("*").alias("total_deliveries"),
            F.sum(
                F.when(
                    F.col("delivery_delay_hours")
                    > self.config.late_threshold_hours,
                    1,
                ).otherwise(0)
            ).alias("late_delivery_count"),
            F.sum(
                F.when(
                    F.col("delivery_delay_hours")
                    <= self.config.late_threshold_hours,
                    1,
                ).otherwise(0)
            ).alias("on_time_delivery_count"),
            F.avg("delivery_delay_hours").alias(
                "average_delay_hours"
            ),
            F.max("delivery_delay_hours").alias(
                "maximum_delay_hours"
            ),
        )

    def aggregate_daily_by_status(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:

        prepared = self.prepare(deliveries)

        if "status" not in prepared.columns:
            raise ValueError(
                "A coluna 'status' é obrigatória para "
                "agregação temporal."
            )

        return (
            prepared.groupBy(
                "event_date",
                "status",
            )
            .agg(
                F.count("*").alias("delivery_event_count"),
                F.countDistinct("delivery_id").alias(
                    "unique_deliveries"
                )
                if "delivery_id" in prepared.columns
                else F.count("*").alias("unique_deliveries"),
            )
            .orderBy(
                "event_date",
                "status",
            )
        )

    def run(
        self,
        deliveries: DataFrame,
    ) -> DataFrame:
        """Executa a agregação principal de entregas."""

        return self.aggregate_overall(deliveries)

    def describe(self) -> dict[str, object]:
        """Retorna a configuração da agregação."""

        return {
            "operation": "delivery_aggregation",
            "event_type": self.config.event_type,
            "late_threshold_hours": self.config.late_threshold_hours,
        }