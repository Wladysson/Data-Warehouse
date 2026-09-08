from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FeatureEngineeringConfig:
    input_path: str = (
        "hdfs://namenode:9000/data/processed/curated"
    )
    output_path: str = (
        "hdfs://namenode:9000/data/processed/curated/features"
    )
    input_format: str = "parquet"
    output_format: str = "parquet"
    write_mode: str = "overwrite"

    def validate(self) -> None:
        if not self.input_path.strip():
            raise ValueError("input_path não pode ser vazio.")

        if not self.output_path.strip():
            raise ValueError("output_path não pode ser vazio.")

        if not self.input_format.strip():
            raise ValueError("input_format não pode ser vazio.")

        if not self.output_format.strip():
            raise ValueError("output_format não pode ser vazio.")

        if not self.write_mode.strip():
            raise ValueError("write_mode não pode ser vazio.")


class FeatureEngineering:

    def __init__(
        self,
        config: Optional[FeatureEngineeringConfig] = None,
    ) -> None:
        self.config = config or FeatureEngineeringConfig()
        self.config.validate()

    def read_curated(
        self,
        spark: Any,
        path: Optional[str] = None,
    ) -> Any:
        if spark is None:
            raise ValueError("spark não pode ser None.")

        input_path = path or self.config.input_path

        if not input_path.strip():
            raise ValueError("O caminho de entrada não pode ser vazio.")

        logger.info(
            "Lendo dados Curated para engenharia de atributos: %s",
            input_path,
        )

        return (
            spark.read
            .format(self.config.input_format)
            .load(input_path)
        )

    def add_customer_activity_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if "customer_id" not in result.columns:
            logger.warning(
                "customer_id não encontrado; "
                "features de atividade não serão criadas."
            )
            return result

        if "event_type" in result.columns:
            result = result.withColumn(
                "is_click_event",
                F.when(
                    F.col("event_type") == "click",
                    1,
                ).otherwise(0),
            )

            result = result.withColumn(
                "is_cart_event",
                F.when(
                    F.col("event_type") == "cart",
                    1,
                ).otherwise(0),
            )

            result = result.withColumn(
                "is_order_event",
                F.when(
                    F.col("event_type") == "order",
                    1,
                ).otherwise(0),
            )

            result = result.withColumn(
                "is_delivery_event",
                F.when(
                    F.col("event_type") == "delivery",
                    1,
                ).otherwise(0),
            )

        return result

    def add_sales_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if "quantity" in result.columns:
            result = result.withColumn(
                "quantity_log",
                F.log1p(
                    F.col("quantity").cast("double")
                ),
            )

        if "unit_price" in result.columns:
            result = result.withColumn(
                "unit_price_log",
                F.log1p(
                    F.col("unit_price").cast("double")
                ),
            )

        if "total_amount" in result.columns:
            result = result.withColumn(
                "total_amount_log",
                F.log1p(
                    F.col("total_amount").cast("double")
                ),
            )

            result = result.withColumn(
                "high_value_order",
                F.when(
                    F.col("total_amount") >= 10000,
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

        from pyspark.sql import functions as F

        result = dataframe

        if "event_timestamp" not in result.columns:
            return result

        result = result.withColumn(
            "event_hour",
            F.hour(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "event_day_of_week",
            F.dayofweek(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "event_day_of_month",
            F.dayofmonth(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "event_month",
            F.month(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "event_year",
            F.year(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "is_weekend",
            F.when(
                F.dayofweek(
                    F.col("event_timestamp")
                ).isin(1, 7),
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "is_business_hour",
            F.when(
                F.hour(
                    F.col("event_timestamp")
                ).between(8, 18),
                1,
            ).otherwise(0),
        )

        return result

    def add_delivery_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if "status" not in result.columns:
            return result

        result = result.withColumn(
            "is_delivery_completed",
            F.when(
                F.col("status") == "delivered",
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "is_delivery_failed",
            F.when(
                F.col("status") == "failed",
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "is_delivery_in_transit",
            F.when(
                F.col("status") == "in_transit",
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "is_out_for_delivery",
            F.when(
                F.col("status") == "out_for_delivery",
                1,
            ).otherwise(0),
        )

        return result

    def add_latency_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if (
            "event_timestamp" not in result.columns
            or "ingestion_timestamp" not in result.columns
        ):
            return result

        result = result.withColumn(
            "event_ingestion_delay_seconds",
            F.unix_timestamp(
                F.col("ingestion_timestamp")
            )
            - F.unix_timestamp(
                F.col("event_timestamp")
            ),
        )

        result = result.withColumn(
            "late_event",
            F.when(
                F.col("event_ingestion_delay_seconds") > 10,
                1,
            ).otherwise(0),
        )

        return result

    def add_behavior_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if (
            "event_type" not in result.columns
            or "action" not in result.columns
        ):
            return result

        result = result.withColumn(
            "is_product_view",
            F.when(
                (
                    F.col("event_type") == "click"
                )
                & (
                    F.col("action") == "product_view"
                ),
                1,
            ).otherwise(0),
        )

        result = result.withColumn(
            "is_add_to_cart_click",
            F.when(
                (
                    F.col("event_type") == "click"
                )
                & (
                    F.col("action") == "add_to_cart"
                ),
                1,
            ).otherwise(0),
        )

        return result

    def build_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        result = self.add_customer_activity_features(
            dataframe
        )

        result = self.add_sales_features(result)
        result = self.add_temporal_features(result)
        result = self.add_delivery_features(result)
        result = self.add_latency_features(result)
        result = self.add_behavior_features(result)

        logger.info(
            "Engenharia de atributos concluída."
        )

        return result

    def write(
        self,
        dataframe: Any,
        path: Optional[str] = None,
        partition_by: Optional[Sequence[str]] = None,
    ) -> None:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        output_path = path or self.config.output_path

        writer = (
            dataframe.write
            .mode(self.config.write_mode)
        )

        if partition_by:
            columns = [
                column.strip()
                for column in partition_by
                if column.strip()
                and column in dataframe.columns
            ]

            if columns:
                writer = writer.partitionBy(*columns)

        (
            writer
            .format(self.config.output_format)
            .save(output_path)
        )

        logger.info(
            "Features persistidas em Parquet: %s",
            output_path,
        )

    def run(
        self,
        spark: Any,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
    ) -> Any:
        dataframe = self.read_curated(
            spark=spark,
            path=input_path,
        )

        features = self.build_features(dataframe)

        self.write(
            dataframe=features,
            path=output_path,
            partition_by=["event_year", "event_month"],
        )

        return features

    def describe(self) -> dict[str, Any]:
        return {
            "input_path": self.config.input_path,
            "output_path": self.config.output_path,
            "input_format": self.config.input_format,
            "output_format": self.config.output_format,
            "write_mode": self.config.write_mode,
        }