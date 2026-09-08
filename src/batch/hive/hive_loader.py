from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True, slots=True)
class HiveLoaderConfig:
    """Configuração do carregamento no Hive."""

    database: str = "ecommerce"
    default_format: str = "parquet"
    default_mode: str = "append"
    partition_column: str = "event_date"


class HiveLoader:

    def __init__(
        self,
        spark: SparkSession,
        config: HiveLoaderConfig | None = None,
    ) -> None:
        if spark is None:
            raise ValueError("spark não pode ser None.")

        self.spark = spark
        self.config = config or HiveLoaderConfig()

    def ensure_database(
        self,
        database: str | None = None,
    ) -> None:
        """Cria o database caso ainda não exista."""

        target_database = database or self.config.database

        if not target_database.strip():
            raise ValueError(
                "database não pode ser vazio."
            )

        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {target_database}"
        )

    def table_exists(
        self,
        table_name: str,
        database: str | None = None,
    ) -> bool:
        """Verifica se uma tabela existe no catálogo Hive."""

        if not table_name.strip():
            raise ValueError(
                "table_name não pode ser vazio."
            )

        target_database = database or self.config.database

        return self.spark.catalog.tableExists(
            f"{target_database}.{table_name}"
        )

    def load(
        self,
        dataframe: DataFrame,
        table_name: str,
        mode: str | None = None,
        format_name: str | None = None,
        partition_by: list[str] | None = None,
        database: str | None = None,
    ) -> None:

        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                "dataframe deve ser uma instância de "
                "pyspark.sql.DataFrame."
            )

        if not table_name.strip():
            raise ValueError(
                "table_name não pode ser vazio."
            )

        target_database = database or self.config.database
        write_mode = mode or self.config.default_mode
        write_format = format_name or self.config.default_format

        self.ensure_database(target_database)

        writer = (
            dataframe.write
            .mode(write_mode)
            .format(write_format)
        )

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.saveAsTable(
            f"{target_database}.{table_name}"
        )

    def load_partitioned(
        self,
        dataframe: DataFrame,
        table_name: str,
        partition_column: str | None = None,
        mode: str | None = None,
        database: str | None = None,
    ) -> None:

        target_partition = (
            partition_column
            or self.config.partition_column
        )

        if target_partition not in dataframe.columns:
            raise ValueError(
                f"Coluna de partição '{target_partition}' "
                "não encontrada no DataFrame."
            )

        self.load(
            dataframe=dataframe,
            table_name=table_name,
            mode=mode,
            partition_by=[target_partition],
            database=database,
        )

    def load_overwrite(
        self,
        dataframe: DataFrame,
        table_name: str,
        partition_by: list[str] | None = None,
        database: str | None = None,
    ) -> None:

        self.load(
            dataframe=dataframe,
            table_name=table_name,
            mode="overwrite",
            partition_by=partition_by,
            database=database,
        )

    def insert_sql(
        self,
        dataframe: DataFrame,
        table_name: str,
        database: str | None = None,
    ) -> None:

        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                "dataframe deve ser uma instância de "
                "pyspark.sql.DataFrame."
            )

        target_database = database or self.config.database

        if not self.table_exists(
            table_name,
            target_database,
        ):
            raise ValueError(
                f"Tabela '{target_database}.{table_name}' "
                "não existe."
            )

        view_name = (
            f"_hive_loader_{table_name}"
        )

        dataframe.createOrReplaceTempView(view_name)

        columns = ", ".join(dataframe.columns)

        self.spark.sql(
            f"""
            INSERT INTO {target_database}.{table_name}
            ({columns})
            SELECT {columns}
            FROM {view_name}
            """
        )

    def read_table(
        self,
        table_name: str,
        database: str | None = None,
    ) -> DataFrame:
        """Lê uma tabela Hive como DataFrame."""

        target_database = database or self.config.database

        if not self.table_exists(
            table_name,
            target_database,
        ):
            raise ValueError(
                f"Tabela '{target_database}.{table_name}' "
                "não existe."
            )

        return self.spark.table(
            f"{target_database}.{table_name}"
        )

    def drop_table(
        self,
        table_name: str,
        database: str | None = None,
    ) -> None:

        target_database = database or self.config.database

        self.spark.sql(
            f"DROP TABLE IF EXISTS "
            f"{target_database}.{table_name}"
        )

    def describe(self) -> dict[str, str]:
        """Retorna a configuração do carregador."""

        return {
            "operation": "hive_loader",
            "database": self.config.database,
            "default_format": self.config.default_format,
            "default_mode": self.config.default_mode,
            "partition_column": self.config.partition_column,
        }