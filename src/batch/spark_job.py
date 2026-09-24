from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

from src.batch.spark_session import (
    SparkSessionConfig,
    SparkSessionFactory,
)
from src.batch.transformations import (
    CartTransformations,
    ClickTransformations,
)
from src.batch.transformations.delivery_transformations import (
    DeliveryTransformations,
)
from src.batch.transformations.sales_transformations import (
    SalesTransformations,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SparkBatchJobConfig:

    application_name: str = (
        "ecommerce-batch-processing"
    )

    input_path: str = (
        "hdfs://namenode:9000/data/raw/events"
    )

    clean_path: str = (
        "hdfs://namenode:9000/data/processed/clean"
    )

    curated_path: str = (
        "hdfs://namenode:9000/data/processed/curated"
    )

    hive_database: str = "ecommerce_dw"

    master: str = "local[*]"

    shuffle_partitions: int = 100

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

        self.click_transformations = (
            ClickTransformations()
        )

        self.cart_transformations = (
            CartTransformations()
        )

        self.sales_transformations = (
            SalesTransformations()
        )

        self.delivery_transformations = (
            DeliveryTransformations()
        )

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

        dataframe = (
            spark.read
            .option(
                "recursiveFileLookup",
                "true",
            )
            .option(
                "pathGlobFilter",
                "*.json",
            )
            .json(input_path)
        )

        return dataframe

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

    def normalize_events(
        self,
        dataframe: Any,
    ) -> Any:

        if dataframe is None:
            raise ValueError(
                "dataframe não pode ser None."
            )

        from pyspark.sql import functions as F

        result = dataframe

        result = result.withColumn(
            "event_timestamp",
            F.to_timestamp(
                F.col("event_timestamp")
            ),
        )

        result = result.withColumn(
            "ingestion_timestamp",
            F.to_timestamp(
                F.col("ingestion_timestamp")
            ),
        )

        result = result.withColumn(
            "event_date",
            F.to_date(
                F.col("event_timestamp")
            ),
        )

        result = result.withColumn(
            "event_type",
            F.lower(
                F.trim(
                    F.col("event_type")
                )
            ),
        )

        return result

    def process_events(
        self,
        dataframe: Any,
    ) -> Dict[str, Any]:

        if dataframe is None:
            raise ValueError(
                "dataframe não pode ser None."
            )

        from pyspark.sql import functions as F

        total_events = dataframe.count()

        if total_events == 0:
            raise RuntimeError(
                "Nenhum evento foi encontrado no HDFS."
            )

        logger.info(
            "Eventos brutos lidos: %s",
            total_events,
        )

        normalized = self.normalize_events(
            dataframe
        )

        click_df = (
            self.click_transformations
            .transform(normalized)
        )

        cart_df = (
            self.cart_transformations
            .transform(normalized)
        )

        sales_df = (
            self.sales_transformations
            .transform(normalized)
        )

        delivery_df = (
            self.delivery_transformations
            .transform(normalized)
        )

        click_count = click_df.count()
        cart_count = cart_df.count()
        sales_count = sales_df.count()
        delivery_count = delivery_df.count()

        logger.info(
            "Eventos click processados: %s",
            click_count,
        )

        logger.info(
            "Eventos cart processados: %s",
            cart_count,
        )

        logger.info(
            "Eventos order processados: %s",
            sales_count,
        )

        logger.info(
            "Eventos delivery processados: %s",
            delivery_count,
        )

        clean_df = (
            normalized
            .withColumn(
                "quantity",
                F.col("quantity").cast("integer"),
            )
            .withColumn(
                "unit_price",
                F.col("unit_price").cast("double"),
            )
            .withColumn(
                "total_amount",
                F.col("total_amount").cast("double"),
            )
        )

        return {
            "normalized": normalized,
            "clean": clean_df,
            "click": click_df,
            "cart": cart_df,
            "sales": sales_df,
            "delivery": delivery_df,
        }

    def create_curated(
        self,
        processed: Dict[str, Any],
    ) -> Any:

        sales_df = processed["sales"]
        click_df = processed["click"]

        self.register_temp_view(
            sales_df,
            "sales_events",
        )

        self.register_temp_view(
            click_df,
            "click_events",
        )

        curated = self.execute_sql(
            """
            SELECT
                event_date,
                COUNT(*) AS total_orders,
                COUNT(DISTINCT order_id) AS unique_orders,
                COUNT(DISTINCT customer_id) AS unique_customers,
                SUM(quantity) AS total_items,
                ROUND(SUM(total_amount), 2) AS total_sales,
                ROUND(AVG(total_amount), 2) AS average_order_value
            FROM sales_events
            GROUP BY event_date
            ORDER BY event_date
            """
        )

        return curated

    def register_hive_tables(
        self,
        processed: Dict[str, Any],
        curated: Any,
    ) -> None:

        if not self.config.enable_hive:
            return

        database = self.config.hive_database

        self.spark.sql(
            f"CREATE DATABASE IF NOT EXISTS `{database}`"
        )

        table_paths = {
            "sales_events": (
                processed["sales"],
                "hdfs://namenode:9000/data/processed/sales",
            ),
            "click_events": (
                processed["click"],
                "hdfs://namenode:9000/data/processed/clicks",
            ),
            "cart_events": (
                processed["cart"],
                "hdfs://namenode:9000/data/processed/carts",
            ),
            "delivery_events": (
                processed["delivery"],
                "hdfs://namenode:9000/data/processed/delivery",
            ),
            "daily_sales_summary": (
                curated,
                "hdfs://namenode:9000/data/processed/curated",
            ),
        }

        for table_name, (dataframe, path) in table_paths.items():

            dataframe.write.mode("overwrite").parquet(path)

            self.spark.sql(
                f"DROP TABLE IF EXISTS "
                f"`{database}`.`{table_name}`"
            )

            self.spark.sql(
                f"""
                CREATE TABLE `{database}`.`{table_name}`
                USING PARQUET
                LOCATION '{path}'
                """
            )

            logger.info(
                "Tabela Hive registrada via Spark: %s.%s",
                database,
                table_name,
            )

        logger.info(
            "Tabelas Hive atualizadas no banco %s via Spark SQL.",
            database,
        )

    def run_pipeline(self) -> Dict[str, Any]:

            self.start()

            logger.info(
                "Iniciando processamento batch."
            )

            raw_df = self.read_json()

            processed = self.process_events(
                raw_df
            )

            self.write_parquet(
                processed["clean"],
                self.config.clean_path,
                mode="overwrite",
                partition_by=["event_date"],
            )

            logger.info(
                "Dataset clean persistido: %s",
                self.config.clean_path,
            )

            curated = self.create_curated(
                processed
            )

            self.write_curated(
                curated,
                self.config.curated_path,
            )

            logger.info(
                "Dataset curated persistido: %s",
                self.config.curated_path,
            )

            self.register_hive_tables(
                processed,
                curated,
            )

            logger.info(
                "Processamento batch concluído."
            )

            return {
                "raw": raw_df,
                "clean": processed["clean"],
                "curated": curated,
                "click": processed["click"],
                "cart": processed["cart"],
                "sales": processed["sales"],
                "delivery": processed["delivery"],
            }

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

    try:
        if not job.healthcheck():
            raise RuntimeError(
                "Não foi possível inicializar "
                "a infraestrutura Spark."
            )

        job.run_pipeline()

    finally:
        job.stop()


if __name__ == "__main__":
    main()