from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SparkSessionConfig:

    application_name: str = (
        "ecommerce-batch-processing"
    )

    master: str = "local[*]"

    shuffle_partitions: int = 4

    warehouse_dir: str = (
        "/user/hive/warehouse"
    )

    hive_metastore_uri: str = (
        "thrift://hive-metastore:9083"
    )

    hdfs_uri: str = (
        "hdfs://namenode:9000"
    )

    timezone: str = "UTC"

    enable_hive_support: bool = True

    def validate(self) -> None:
        if not self.application_name.strip():
            raise ValueError(
                "application_name não pode ser vazio."
            )

        if not self.master.strip():
            raise ValueError(
                "master não pode ser vazio."
            )

        if self.shuffle_partitions < 1:
            raise ValueError(
                "shuffle_partitions deve ser maior que zero."
            )

        if not self.warehouse_dir.strip():
            raise ValueError(
                "warehouse_dir não pode ser vazio."
            )

        if not self.hive_metastore_uri.strip():
            raise ValueError(
                "hive_metastore_uri não pode ser vazio."
            )

        if not self.hdfs_uri.strip():
            raise ValueError(
                "hdfs_uri não pode ser vazio."
            )

        if not self.timezone.strip():
            raise ValueError(
                "timezone não pode ser vazio."
            )


class SparkSessionFactory:

    def __init__(
        self,
        config: Optional[
            SparkSessionConfig
        ] = None,
    ) -> None:
        self.config = (
            config
            or SparkSessionConfig()
        )

        self.config.validate()

        self._session: Any = None

    @property
    def session(self) -> Any:
        return self._session

    @property
    def is_created(self) -> bool:
        return self._session is not None

    def build_configuration(
        self,
    ) -> Dict[str, str]:

        self.config.validate()

        return {
            "spark.app.name": (
                self.config.application_name
            ),
            "spark.master": (
                self.config.master
            ),
            "spark.sql.shuffle.partitions": (
                str(
                    self.config.shuffle_partitions
                )
            ),
            "spark.sql.warehouse.dir": (
                self.config.warehouse_dir
            ),
            "spark.hadoop.hive.metastore.uris": (
                self.config.hive_metastore_uri
            ),
            "spark.hadoop.fs.defaultFS": (
                self.config.hdfs_uri
            ),
            "spark.sql.session.timeZone": (
                self.config.timezone
            ),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": (
                "true"
            ),
            "spark.sql.adaptive.skewJoin.enabled": (
                "true"
            ),
            "spark.serializer": (
                "org.apache.spark.serializer.KryoSerializer"
            ),
            "spark.sql.sources.partitionOverwriteMode": (
                "dynamic"
            ),
        }

    def create(self) -> Any:

        if self._session is not None:
            return self._session

        try:
            from pyspark.sql import SparkSession

        except ImportError as exc:
            raise RuntimeError(
                "O processamento batch requer "
                "o pacote pyspark."
            ) from exc

        builder = SparkSession.builder

        configuration = (
            self.build_configuration()
        )

        for key, value in configuration.items():
            builder = builder.config(
                key,
                value,
            )

        if self.config.enable_hive_support:
            builder = builder.enableHiveSupport()

        self._session = builder.getOrCreate()

        logger.info(
            "SparkSession criada: app=%s, master=%s.",
            self.config.application_name,
            self.config.master,
        )

        return self._session

    def get_or_create(self) -> Any:
        return self.create()

    def stop(self) -> None:

        if self._session is None:
            return

        self._session.stop()

        logger.info(
            "SparkSession encerrada: app=%s.",
            self.config.application_name,
        )

        self._session = None

    def healthcheck(self) -> bool:

        try:
            session = self.create()

            return (
                session is not None
                and not session.sparkContext._jsc.sc().isStopped()
            )

        except Exception as exc:
            logger.warning(
                "Healthcheck Spark falhou: %s",
                exc,
            )
            return False

    def describe(self) -> Dict[str, Any]:
        return {
            "application_name": (
                self.config.application_name
            ),
            "master": self.config.master,
            "shuffle_partitions": (
                self.config.shuffle_partitions
            ),
            "warehouse_dir": (
                self.config.warehouse_dir
            ),
            "hive_metastore_uri": (
                self.config.hive_metastore_uri
            ),
            "hdfs_uri": self.config.hdfs_uri,
            "timezone": self.config.timezone,
            "hive_support": (
                self.config.enable_hive_support
            ),
            "session_created": self.is_created,
        }