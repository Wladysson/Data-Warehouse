from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True, slots=True)
class TableManagerConfig:

    database: str = "ecommerce"
    default_format: str = "PARQUET"


class TableManager:

    def __init__(
        self,
        spark: SparkSession,
        config: TableManagerConfig | None = None,
    ) -> None:
        if spark is None:
            raise ValueError("spark não pode ser None.")

        self.spark = spark
        self.config = config or TableManagerConfig()

    @staticmethod
    def _validate_identifier(
        value: str,
        name: str,
    ) -> None:
        if not value.strip():
            raise ValueError(
                f"{name} não pode ser vazio."
            )

    def ensure_database(
        self,
        database: str | None = None,
    ) -> None:

        target_database = database or self.config.database

        self._validate_identifier(
            target_database,
            "database",
        )

        self.spark.sql(
            f"""
            CREATE DATABASE IF NOT EXISTS
            {target_database}
            """
        )

    def table_exists(
        self,
        table_name: str,
        database: str | None = None,
    ) -> bool:
        """Verifica a existência de uma tabela."""

        self._validate_identifier(
            table_name,
            "table_name",
        )

        target_database = database or self.config.database

        return self.spark.catalog.tableExists(
            f"{target_database}.{table_name}"
        )

    def create_table(
        self,
        table_name: str,
        columns: dict[str, str],
        partition_columns: list[str] | None = None,
        database: str | None = None,
        table_format: str | None = None,
    ) -> None:

        self._validate_identifier(
            table_name,
            "table_name",
        )

        if not columns:
            raise ValueError(
                "columns não pode ser vazio."
            )

        target_database = database or self.config.database
        storage_format = (
            table_format
            or self.config.default_format
        )

        self.ensure_database(target_database)

        column_definition = ", ".join(
            f"{name} {data_type}"
            for name, data_type in columns.items()
        )

        partition_clause = ""

        if partition_columns:
            missing = set(partition_columns).difference(
                columns
            )

            if missing:
                raise ValueError(
                    "Colunas de partição ausentes na definição "
                    f"da tabela: {sorted(missing)}."
                )

            partition_definition = ", ".join(
                f"{column} {columns[column]}"
                for column in partition_columns
            )

            partition_clause = (
                f"PARTITIONED BY ({partition_definition})"
            )

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS
            {target_database}.{table_name}
            ({column_definition})
            {partition_clause}
            STORED AS {storage_format}
            """
        )

    def create_external_table(
        self,
        table_name: str,
        columns: dict[str, str],
        location: str,
        partition_columns: list[str] | None = None,
        database: str | None = None,
        table_format: str | None = None,
    ) -> None:

        self._validate_identifier(
            table_name,
            "table_name",
        )

        self._validate_identifier(
            location,
            "location",
        )

        if not columns:
            raise ValueError(
                "columns não pode ser vazio."
            )

        target_database = database or self.config.database
        storage_format = (
            table_format
            or self.config.default_format
        )

        self.ensure_database(target_database)

        column_definition = ", ".join(
            f"{name} {data_type}"
            for name, data_type in columns.items()
            if not partition_columns
            or name not in partition_columns
        )

        partition_clause = ""

        if partition_columns:
            missing = set(partition_columns).difference(
                columns
            )

            if missing:
                raise ValueError(
                    "Colunas de partição ausentes na definição "
                    f"da tabela: {sorted(missing)}."
                )

            partition_definition = ", ".join(
                f"{column} {columns[column]}"
                for column in partition_columns
            )

            partition_clause = (
                f"PARTITIONED BY ({partition_definition})"
            )

        self.spark.sql(
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS
            {target_database}.{table_name}
            ({column_definition})
            {partition_clause}
            STORED AS {storage_format}
            LOCATION '{location}'
            """
        )

    def describe_table(
        self,
        table_name: str,
        database: str | None = None,
    ):

        target_database = database or self.config.database

        if not self.table_exists(
            table_name,
            target_database,
        ):
            raise ValueError(
                f"Tabela '{target_database}.{table_name}' "
                "não existe."
            )

        return self.spark.sql(
            f"""
            DESCRIBE
            {target_database}.{table_name}
            """
        )

    def show_tables(
        self,
        database: str | None = None,
    ):
        target_database = database or self.config.database

        self.ensure_database(target_database)

        return self.spark.sql(
            f"SHOW TABLES IN {target_database}"
        )

    def truncate_table(
        self,
        table_name: str,
        database: str | None = None,
    ) -> None:

        target_database = database or self.config.database

        if not self.table_exists(
            table_name,
            target_database,
        ):
            raise ValueError(
                f"Tabela '{target_database}.{table_name}' "
                "não existe."
            )

        self.spark.sql(
            f"""
            TRUNCATE TABLE
            {target_database}.{table_name}
            """
        )

    def drop_table(
        self,
        table_name: str,
        database: str | None = None,
        purge: bool = False,
    ) -> None:

        target_database = database or self.config.database

        purge_clause = " PURGE" if purge else ""

        self.spark.sql(
            f"""
            DROP TABLE IF EXISTS
            {target_database}.{table_name}
            {purge_clause}
            """
        )

    def rename_table(
        self,
        table_name: str,
        new_table_name: str,
        database: str | None = None,
    ) -> None:

        self._validate_identifier(
            table_name,
            "table_name",
        )

        self._validate_identifier(
            new_table_name,
            "new_table_name",
        )

        target_database = database or self.config.database

        self.spark.sql(
            f"""
            ALTER TABLE
            {target_database}.{table_name}
            RENAME TO
            {target_database}.{new_table_name}
            """
        )

    def analyze_table(
        self,
        table_name: str,
        database: str | None = None,
    ) -> None:

        target_database = database or self.config.database

        self.spark.sql(
            f"""
            ANALYZE TABLE
            {target_database}.{table_name}
            COMPUTE STATISTICS
            """
        )

    def describe(self) -> dict[str, str]:

        return {
            "operation": "table_manager",
            "database": self.config.database,
            "default_format": self.config.default_format,
        }