from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True, slots=True)
class PartitionManagerConfig:
    """Configuração do gerenciamento de partições."""

    database: str = "ecommerce"
    partition_column: str = "event_date"


class PartitionManager:
    
    def __init__(
        self,
        spark: SparkSession,
        config: PartitionManagerConfig | None = None,
    ) -> None:
        if spark is None:
            raise ValueError("spark não pode ser None.")

        self.spark = spark
        self.config = config or PartitionManagerConfig()

    @staticmethod
    def _validate_identifier(
        value: str,
        name: str,
    ) -> None:
        if not value.strip():
            raise ValueError(
                f"{name} não pode ser vazio."
            )

    def add_partition(
        self,
        table_name: str,
        partition_spec: dict[str, str],
        database: str | None = None,
        location: str | None = None,
    ) -> None:

        self._validate_identifier(
            table_name,
            "table_name",
        )

        if not partition_spec:
            raise ValueError(
                "partition_spec não pode ser vazio."
            )

        target_database = database or self.config.database

        specification = ", ".join(
            f"{key}='{value}'"
            for key, value in partition_spec.items()
        )

        location_clause = (
            f" LOCATION '{location}'"
            if location
            else ""
        )

        self.spark.sql(
            f"""
            ALTER TABLE {target_database}.{table_name}
            ADD IF NOT EXISTS
            PARTITION ({specification})
            {location_clause}
            """
        )

    def drop_partition(
        self,
        table_name: str,
        partition_spec: dict[str, str],
        database: str | None = None,
    ) -> None:
        """Remove uma partição de uma tabela Hive."""

        self._validate_identifier(
            table_name,
            "table_name",
        )

        if not partition_spec:
            raise ValueError(
                "partition_spec não pode ser vazio."
            )

        target_database = database or self.config.database

        specification = ", ".join(
            f"{key}='{value}'"
            for key, value in partition_spec.items()
        )

        self.spark.sql(
            f"""
            ALTER TABLE {target_database}.{table_name}
            DROP IF EXISTS PARTITION ({specification})
            """
        )

    def show_partitions(
        self,
        table_name: str,
        database: str | None = None,
    ) -> list[str]:

        self._validate_identifier(
            table_name,
            "table_name",
        )

        target_database = database or self.config.database

        rows = self.spark.sql(
            f"""
            SHOW PARTITIONS
            {target_database}.{table_name}
            """
        ).collect()

        return [
            row[0]
            for row in rows
        ]

    def repair_table(
        self,
        table_name: str,
        database: str | None = None,
    ) -> None:


        self._validate_identifier(
            table_name,
            "table_name",
        )

        target_database = database or self.config.database

        self.spark.sql(
            f"""
            MSCK REPAIR TABLE
            {target_database}.{table_name}
            """
        )

    def recover_partitions(
        self,
        table_name: str,
        database: str | None = None,
    ) -> None:

        self.repair_table(
            table_name=table_name,
            database=database,
        )

    def partition_exists(
        self,
        table_name: str,
        partition_spec: dict[str, str],
        database: str | None = None,
    ) -> bool:

        target_database = database or self.config.database

        partitions = self.show_partitions(
            table_name=table_name,
            database=target_database,
        )

        expected = "/".join(
            f"{key}={value}"
            for key, value in partition_spec.items()
        )

        return any(
            partition == expected
            for partition in partitions
        )

    def drop_partition_if_exists(
        self,
        table_name: str,
        partition_spec: dict[str, str],
        database: str | None = None,
    ) -> bool:

        if not self.partition_exists(
            table_name,
            partition_spec,
            database,
        ):
            return False

        self.drop_partition(
            table_name,
            partition_spec,
            database,
        )

        return True

    def describe(self) -> dict[str, str]:

        return {
            "operation": "partition_manager",
            "database": self.config.database,
            "partition_column": self.config.partition_column,
        }