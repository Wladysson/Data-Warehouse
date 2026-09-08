from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RawToCleanConfig:
    input_path: str = "hdfs://namenode:9000/data/raw"
    output_path: str = "hdfs://namenode:9000/data/processed/clean"
    input_format: str = "json"
    output_format: str = "parquet"
    write_mode: str = "overwrite"
    event_timestamp_column: str = "event_timestamp"
    ingestion_timestamp_column: str = "ingestion_timestamp"

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

        if not self.event_timestamp_column.strip():
            raise ValueError("event_timestamp_column não pode ser vazio.")

        if not self.ingestion_timestamp_column.strip():
            raise ValueError("ingestion_timestamp_column não pode ser vazio.")


class RawToClean:
    
    def __init__(
        self,
        config: Optional[RawToCleanConfig] = None,
    ) -> None:
        self.config = config or RawToCleanConfig()
        self.config.validate()

    def read_raw(self, spark: Any, path: Optional[str] = None) -> Any:
        if spark is None:
            raise ValueError("spark não pode ser None.")

        input_path = path or self.config.input_path

        if not input_path.strip():
            raise ValueError("O caminho de entrada não pode ser vazio.")

        logger.info("Lendo dados Raw do HDFS: %s", input_path)

        return (
            spark.read
            .format(self.config.input_format)
            .load(input_path)
        )

    def normalize_columns(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        normalized = dataframe

        for column in dataframe.columns:
            normalized_name = column.strip().lower()

            if normalized_name != column:
                normalized = normalized.withColumnRenamed(
                    column,
                    normalized_name,
                )

        logger.debug("Colunas normalizadas para o padrão da camada Clean.")

        return normalized

    def cast_timestamp_columns(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        for column in (
            self.config.event_timestamp_column,
            self.config.ingestion_timestamp_column,
        ):
            if column in result.columns:
                result = result.withColumn(
                    column,
                    F.to_timestamp(F.col(column)),
                )

        return result

    def remove_invalid_events(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        required_columns = [
            "event_id",
            "event_type",
            self.config.event_timestamp_column,
            self.config.ingestion_timestamp_column,
            "customer_id",
        ]

        existing_required = [
            column
            for column in required_columns
            if column in dataframe.columns
        ]

        if not existing_required:
            logger.warning(
                "Nenhuma coluna obrigatória foi encontrada; "
                "nenhuma filtragem estrutural será aplicada."
            )
            return dataframe

        condition = F.lit(True)

        for column in existing_required:
            condition = condition & F.col(column).isNotNull()

        return dataframe.filter(condition)

    def drop_duplicates(
        self,
        dataframe: Any,
        subset: Optional[Sequence[str]] = None,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        columns = [
            column
            for column in (subset or ["event_id"])
            if column in dataframe.columns
        ]

        if not columns:
            return dataframe.dropDuplicates()

        return dataframe.dropDuplicates(columns)

    def clean(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        result = self.normalize_columns(dataframe)
        result = self.cast_timestamp_columns(result)
        result = self.remove_invalid_events(result)
        result = self.drop_duplicates(result)

        logger.info(
            "Transformação Raw → Clean concluída."
        )

        return result

    def write_clean(
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
            "Dados Clean persistidos em Parquet: %s",
            output_path,
        )

    def run(
        self,
        spark: Any,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
        partition_by: Optional[Sequence[str]] = None,
    ) -> Any:
        dataframe = self.read_raw(
            spark=spark,
            path=input_path,
        )

        clean_dataframe = self.clean(dataframe)

        self.write_clean(
            dataframe=clean_dataframe,
            path=output_path,
            partition_by=partition_by,
        )

        return clean_dataframe

    def describe(self) -> dict[str, Any]:
        return {
            "input_path": self.config.input_path,
            "output_path": self.config.output_path,
            "input_format": self.config.input_format,
            "output_format": self.config.output_format,
            "write_mode": self.config.write_mode,
            "event_timestamp_column": self.config.event_timestamp_column,
            "ingestion_timestamp_column": (
                self.config.ingestion_timestamp_column
            ),
        }