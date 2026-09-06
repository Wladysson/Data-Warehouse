from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

from .spark_session import (
    SparkSessionConfig,
    SparkSessionFactory,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SparkBatchJobConfig:

    application_name: str = (
        "ecommerce-batch-processing"
    )

    input_path: str = (
        "hdfs://namenode:9000/data/raw"
    )

    clean_path: str = (
        "hdfs://namenode:9000/data/processed/clean"
    )

    curated_path: str = (
        "hdfs://namenode:9000/data/processed/curated"
    )

    hive_database: str = "ecommerce_dw"

    master: str = "local[*]"

    shuffle_partitions: int = 4

    enable_hive: bool = True

    def validate(self) -> None:
        if not self.application_name.strip():
            raise ValueError(
                "application_name não pode ser vazio."
            )

        if not self.input_path.strip():
            raise ValueError(
                "input_path não pode ser vazio."
            )

        if not self.clean_path.strip():
            raise ValueError(
                "clean_path não pode ser vazio."
            )

        if not self.curated_path.strip():
            raise ValueError(
                "curated_path não pode ser vazio."
            )

        if not self.hive_database.strip():
            raise ValueError(
                "hive_database não pode ser vazio."
            )

        if not self.master.strip():
            raise ValueError(
                "master não pode ser vazio."
            )

        if self.shuffle_partitions < 1:
            raise ValueError(
                "shuffle_partitions deve ser maior que zero."
            )


class SparkBatchJob:

    def __init__(
        self,
        config: Optional[
            SparkBatchJobConfig
        ] = None,
        session_factory: Optional[
            SparkSessionFactory
        ] = None,
    ) -> None:
        self.config = (
            config
            or SparkBatchJobConfig()
        )

        self.config.validate()

        self.session_factory = (
            session_factory
            or SparkSessionFactory(
                SparkSessionConfig(
                    application_name=(
                        self.config.application_name
                    ),
                    master=self.config.master,
                    shuffle_partitions=(
                        self.config.shuffle_partitions
                    ),
                    enable_hive_support=(
                        self.config.enable_hive
                    ),
                )
            )
        )

        self._spark: Any = None
        self._running = False

    @property
    def spark(self) -> Any:
        return self._spark

    @property
    def running(self) -> bool:
        return self._running

    def create_session(self) -> Any:

        self.config.validate()

        self._spark = (
            self.session_factory.get_or_create()
        )

        return self._spark

    def ensure_database(self) -> None:

        spark = self.create_session()

        if not self.config.enable_hive:
            logger.info(
                "Hive desabilitado; banco analítico não será criado."
            )
            return

        database_name = (
            self.config.hive_database
        )

        spark.sql(
            f"CREATE DATABASE IF NOT EXISTS "
            f"`{database_name}`"
        )

        logger.info(
            "Banco Hive disponível: %s",
            database_name,
        )

    def read_json(
        self,
        path: Optional[str] = None,
    ) -> Any:

        spark = self.create_session()

        input_path = (
            path
            or self.config.input_path
        )

        if not input_path.strip():
            raise ValueError(
                "O caminho de entrada não pode ser vazio."
            )

        logger.info(
            "Lendo dados JSON do caminho: %s",
            input_path,
        )

        return spark.read.json(
            input_path
        )

    def read_parquet(
        self,
        path: Optional[str] = None,
    ) -> Any:

        spark = self.create_session()

        input_path = (
            path
            or self.config.input_path
        )

        if not input_path.strip():
            raise ValueError(
                "O caminho de entrada não pode ser vazio."
            )

        logger.info(
            "Lendo dados Parquet do caminho: %s",
            input_path,
        )

        return spark.read.parquet(
            input_path
        )

    def write_parquet(
        self,
        dataframe: Any,
        path: str,
        mode: str = "overwrite",
        partition_by: Optional[
            Sequence[str]
        ] = None,
    ) -> None:

        if dataframe is None:
            raise ValueError(
                "dataframe não pode ser None."
            )

        if not path.strip():
            raise ValueError(
                "path não pode ser vazio."
            )

        if not mode.strip():
            raise ValueError(
                "mode não pode ser vazio."
            )

        writer = (
            dataframe.write
            .mode(mode)
        )

        if partition_by:
            normalized_partitions = [
                column.strip()
                for column in partition_by
                if column.strip()
            ]

            if normalized_partitions:
                writer = writer.partitionBy(
                    *normalized_partitions
                )

        writer.parquet(path)

        logger.info(
            "DataFrame persistido em Parquet: %s",
            path,
        )

    def register_temp_view(
        self,
        dataframe: Any,
        view_name: str,
    ) -> None:

        if dataframe is None:
            raise ValueError(
                "dataframe não pode ser None."
            )

        normalized_name = view_name.strip()

        if not normalized_name:
            raise ValueError(
                "view_name não pode ser vazio."
            )

        dataframe.createOrReplaceTempView(
            normalized_name
        )

        logger.debug(
            "Temporary View registrada: %s",
            normalized_name,
        )

    def execute_sql(
        self,
        query: str,
    ) -> Any:

        if not query.strip():
            raise ValueError(
                "query não pode ser vazia."
            )

        spark = self.create_session()

        logger.debug(
            "Executando Spark SQL."
        )

        return spark.sql(query)

    def write_curated(
        self,
        dataframe: Any,
        path: Optional[str] = None,
        partition_by: Optional[
            Sequence[str]
        ] = None,
    ) -> None:

        output_path = (
            path
            or self.config.curated_path
        )

        self.write_parquet(
            dataframe=dataframe,
            path=output_path,
            mode="overwrite",
            partition_by=partition_by,
        )

    def run_sql_pipeline(
        self,
        queries: Sequence[str],
    ) -> list[Any]:

        if not queries:
            raise ValueError(
                "queries não pode estar vazia."
            )

        results: list[Any] = []

        for query in queries:
            results.append(
                self.execute_sql(query)
            )

        return results

    def start(self) -> None:

        if self._running:
            logger.warning(
                "O job Spark '%s' já está em execução.",
                self.config.application_name,
            )
            return

        self.config.validate()

        self.create_session()
        self.ensure_database()

        self._running = True

        logger.info(
            "Job Spark '%s' iniciado.",
            self.config.application_name,
        )

    def stop(self) -> None:
        
        if not self._running and self._spark is None:
            return

        logger.info(
            "Encerrando job Spark '%s'.",
            self.config.application_name,
        )

        self._running = False

        self.session_factory.stop()

        self._spark = None

    def healthcheck(self) -> bool:

        try:
            self.config.validate()

            return (
                self.session_factory.healthcheck()
            )

        except Exception as exc:
            logger.warning(
                "Healthcheck do job Spark falhou: %s",
                exc,
            )
            return False

    def describe(self) -> Dict[str, Any]:
        return {
            "application_name": (
                self.config.application_name
            ),
            "input_path": (
                self.config.input_path
            ),
            "clean_path": (
                self.config.clean_path
            ),
            "curated_path": (
                self.config.curated_path
            ),
            "hive_database": (
                self.config.hive_database
            ),
            "master": self.config.master,
            "shuffle_partitions": (
                self.config.shuffle_partitions
            ),
            "hive_enabled": (
                self.config.enable_hive
            ),
            "running": self._running,
            "session_created": (
                self._spark is not None
            ),
        }


def create_spark_job(
    config: Optional[
        SparkBatchJobConfig
    ] = None,
) -> SparkBatchJob:
    return SparkBatchJob(
        config=config
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "[%(name)s] %(message)s"
        ),
    )

    job = create_spark_job()

    if not job.healthcheck():
        raise RuntimeError(
            "Não foi possível inicializar "
            "a infraestrutura Spark."
        )

    job.start()

    logger.info(
        "Job Spark '%s' pronto para processamento batch.",
        job.config.application_name,
    )

    job.stop()


if __name__ == "__main__":
    main()