from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CleanToCuratedConfig:
    input_path: str = (
        "hdfs://namenode:9000/data/processed/clean"
    )
    output_path: str = (
        "hdfs://namenode:9000/data/processed/curated"
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


class CleanToCurated:
    
    def __init__(
        self,
        config: Optional[CleanToCuratedConfig] = None,
    ) -> None:
        self.config = config or CleanToCuratedConfig()
        self.config.validate()

    def read_clean(
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
            "Lendo dados Clean do HDFS: %s",
            input_path,
        )

        return (
            spark.read
            .format(self.config.input_format)
            .load(input_path)
        )

    def add_event_date(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        if "event_timestamp" not in dataframe.columns:
            logger.warning(
                "Coluna event_timestamp não encontrada; "
                "event_date não será criada."
            )
            return dataframe

        return dataframe.withColumn(
            "event_date",
            F.to_date(F.col("event_timestamp")),
        )

    def add_event_hour(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        if "event_timestamp" not in dataframe.columns:
            logger.warning(
                "Coluna event_timestamp não encontrada; "
                "event_hour não será criada."
            )
            return dataframe

        return dataframe.withColumn(
            "event_hour",
            F.hour(F.col("event_timestamp")),
        )

    def add_order_metrics(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if (
            "quantity" in result.columns
            and "unit_price" in result.columns
            and "total_amount" not in result.columns
        ):
            result = result.withColumn(
                "total_amount",
                F.round(
                    F.col("quantity") * F.col("unit_price"),
                    2,
                ),
            )

        if "total_amount" in result.columns:
            result = result.withColumn(
                "total_amount",
                F.round(
                    F.col("total_amount").cast("double"),
                    2,
                ),
            )

        return result

    def add_processing_metadata(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if "event_timestamp" in result.columns:
            result = result.withColumn(
                "event_year",
                F.year(F.col("event_timestamp")),
            )

            result = result.withColumn(
                "event_month",
                F.month(F.col("event_timestamp")),
            )

            result = result.withColumn(
                "event_day",
                F.dayofmonth(F.col("event_timestamp")),
            )

        return result

    def select_curated_columns(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        preferred_columns = [
            "event_id",
            "event_type",
            "event_timestamp",
            "ingestion_timestamp",
            "customer_id",
            "session_id",
            "product_id",
            "order_id",
            "cart_id",
            "delivery_id",
            "quantity",
            "unit_price",
            "total_amount",
            "status",
            "carrier",
            "page",
            "action",
            "event_date",
            "event_hour",
            "event_year",
            "event_month",
            "event_day",
        ]

        selected_columns = [
            column
            for column in preferred_columns
            if column in dataframe.columns
        ]

        if not selected_columns:
            return dataframe

        return dataframe.select(*selected_columns)

    def curate(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        result = self.add_event_date(dataframe)
        result = self.add_event_hour(result)
        result = self.add_order_metrics(result)
        result = self.add_processing_metadata(result)
        result = self.select_curated_columns(result)

        logger.info(
            "Transformação Clean → Curated concluída."
        )

        return result

    def write_curated(
        self,
        dataframe: Any,
        path: Optional[str] = None,
        partition_by: Optional[Sequence[str]] = None,
    ) -> None:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        output_path = path or self.config.output_path

        if not output_path.strip():
            raise ValueError("O caminho de saída não pode ser vazio.")

        writer = dataframe.write.mode(self.config.write_mode)

        if partition_by:
            columns = [
                column.strip()
                for column in partition_by
                if column.strip() and column in dataframe.columns
            ]

            if columns:
                writer = writer.partitionBy(*columns)

        writer.format(self.config.output_format).save(output_path)

        logger.info(
            "Dados Curated persistidos em Parquet: %s",
            output_path,
        )

    def run(
        self,
        spark: Any,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
        partition_by: Optional[Sequence[str]] = None,
    ) -> Any:
        dataframe = self.read_clean(
            spark=spark,
            path=input_path,
        )

        curated_dataframe = self.curate(dataframe)

        self.write_curated(
            dataframe=curated_dataframe,
            path=output_path,
            partition_by=partition_by,
        )

        return curated_dataframe

    def describe(self) -> dict[str, Any]:
        return {
            "input_path": self.config.input_path,
            "output_path": self.config.output_path,
            "input_format": self.config.input_format,
            "output_format": self.config.output_format,
            "write_mode": self.config.write_mode,
        }