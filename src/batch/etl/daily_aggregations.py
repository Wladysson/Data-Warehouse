from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DailyAggregationConfig:
    input_path: str = (
        "hdfs://namenode:9000/data/processed/curated"
    )
    output_path: str = (
        "hdfs://namenode:9000/data/processed/curated/daily"
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


class DailyAggregations:

    def __init__(
        self,
        config: Optional[DailyAggregationConfig] = None,
    ) -> None:
        self.config = config or DailyAggregationConfig()
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
            "Lendo dados Curated para agregação diária: %s",
            input_path,
        )

        return (
            spark.read
            .format(self.config.input_format)
            .load(input_path)
        )

    def prepare_date_column(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        if "event_date" in dataframe.columns:
            return dataframe

        if "event_timestamp" not in dataframe.columns:
            raise ValueError(
                "O DataFrame precisa possuir event_date ou "
                "event_timestamp."
            )

        return dataframe.withColumn(
            "event_date",
            F.to_date(F.col("event_timestamp")),
        )

    def aggregate_events_by_day(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        dataframe = self.prepare_date_column(dataframe)

        aggregations = [
            F.count("*").alias("total_events"),
        ]

        if "customer_id" in dataframe.columns:
            aggregations.append(
                F.countDistinct("customer_id").alias(
                    "unique_customers"
                )
            )

        if "product_id" in dataframe.columns:
            aggregations.append(
                F.countDistinct("product_id").alias(
                    "unique_products"
                )
            )

        return (
            dataframe
            .groupBy("event_date")
            .agg(*aggregations)
            .orderBy("event_date")
        )

    def aggregate_sales_by_day(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        dataframe = self.prepare_date_column(dataframe)

        if "event_type" not in dataframe.columns:
            raise ValueError(
                "O DataFrame precisa possuir event_type."
            )

        sales_dataframe = dataframe.filter(
            F.col("event_type") == "order"
        )

        aggregations = [
            F.count("*").alias("total_orders"),
        ]

        if "quantity" in sales_dataframe.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_items_sold"
                )
            )

        if "total_amount" in sales_dataframe.columns:
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
                    ).alias("highest_order_value"),
                ]
            )

        return (
            sales_dataframe
            .groupBy("event_date")
            .agg(*aggregations)
            .orderBy("event_date")
        )

    def aggregate_clicks_by_day(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        dataframe = self.prepare_date_column(dataframe)

        if "event_type" not in dataframe.columns:
            raise ValueError(
                "O DataFrame precisa possuir event_type."
            )

        clicks = dataframe.filter(
            F.col("event_type") == "click"
        )

        aggregations = [
            F.count("*").alias("total_clicks"),
        ]

        if "customer_id" in clicks.columns:
            aggregations.append(
                F.countDistinct("customer_id").alias(
                    "unique_click_customers"
                )
            )

        if "product_id" in clicks.columns:
            aggregations.append(
                F.countDistinct("product_id").alias(
                    "unique_clicked_products"
                )
            )

        return (
            clicks
            .groupBy("event_date")
            .agg(*aggregations)
            .orderBy("event_date")
        )

    def aggregate_carts_by_day(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        dataframe = self.prepare_date_column(dataframe)

        if "event_type" not in dataframe.columns:
            raise ValueError(
                "O DataFrame precisa possuir event_type."
            )

        carts = dataframe.filter(
            F.col("event_type") == "cart"
        )

        aggregations = [
            F.count("*").alias("total_cart_events"),
        ]

        if "cart_id" in carts.columns:
            aggregations.append(
                F.countDistinct("cart_id").alias(
                    "unique_carts"
                )
            )

        if "quantity" in carts.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_cart_items"
                )
            )

        return (
            carts
            .groupBy("event_date")
            .agg(*aggregations)
            .orderBy("event_date")
        )

    def aggregate_deliveries_by_day(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        dataframe = self.prepare_date_column(dataframe)

        if "event_type" not in dataframe.columns:
            raise ValueError(
                "O DataFrame precisa possuir event_type."
            )

        deliveries = dataframe.filter(
            F.col("event_type") == "delivery"
        )

        aggregations = [
            F.count("*").alias("total_delivery_events"),
        ]

        if "order_id" in deliveries.columns:
            aggregations.append(
                F.countDistinct("order_id").alias(
                    "unique_delivery_orders"
                )
            )

        if "status" in deliveries.columns:
            aggregations.append(
                F.sum(
                    F.when(
                        F.col("status") == "delivered",
                        1,
                    ).otherwise(0)
                ).alias("delivered_orders")
            )

            aggregations.append(
                F.sum(
                    F.when(
                        F.col("status") == "failed",
                        1,
                    ).otherwise(0)
                ).alias("failed_orders")
            )

        return (
            deliveries
            .groupBy("event_date")
            .agg(*aggregations)
            .orderBy("event_date")
        )

    def aggregate_by_event_type(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        dataframe = self.prepare_date_column(dataframe)

        if "event_type" not in dataframe.columns:
            raise ValueError(
                "O DataFrame precisa possuir event_type."
            )

        return (
            dataframe
            .groupBy(
                "event_date",
                "event_type",
            )
            .agg(
                F.count("*").alias("event_count"),
                F.countDistinct(
                    "customer_id"
                ).alias("unique_customers"),
            )
            .orderBy(
                "event_date",
                "event_type",
            )
        )

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
            "Agregações diárias persistidas: %s",
            output_path,
        )

    def run(
        self,
        spark: Any,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
    ) -> dict[str, Any]:
        dataframe = self.read_curated(
            spark=spark,
            path=input_path,
        )

        events = self.aggregate_events_by_day(dataframe)
        sales = self.aggregate_sales_by_day(dataframe)
        clicks = self.aggregate_clicks_by_day(dataframe)
        carts = self.aggregate_carts_by_day(dataframe)
        deliveries = self.aggregate_deliveries_by_day(dataframe)

        base_path = output_path or self.config.output_path

        self.write(
            events,
            f"{base_path}/events",
            partition_by=["event_date"],
        )

        self.write(
            sales,
            f"{base_path}/sales",
            partition_by=["event_date"],
        )

        self.write(
            clicks,
            f"{base_path}/clicks",
            partition_by=["event_date"],
        )

        self.write(
            carts,
            f"{base_path}/carts",
            partition_by=["event_date"],
        )

        self.write(
            deliveries,
            f"{base_path}/deliveries",
            partition_by=["event_date"],
        )

        logger.info(
            "Processamento de agregações diárias concluído."
        )

        return {
            "events": events,
            "sales": sales,
            "clicks": clicks,
            "carts": carts,
            "deliveries": deliveries,
        }

    def describe(self) -> dict[str, Any]:
        return {
            "input_path": self.config.input_path,
            "output_path": self.config.output_path,
            "input_format": self.config.input_format,
            "output_format": self.config.output_format,
            "write_mode": self.config.write_mode,
        }