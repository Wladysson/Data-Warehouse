from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FlinkJobConfig:

    job_name: str = "ecommerce-streaming-job"
    parallelism: int = 1

    watermark_out_of_orderness_seconds: int = 10

    sliding_window_size_seconds: int = 60
    sliding_window_slide_seconds: int = 10

    checkpoint_interval_ms: int = 60_000

    input_path: str = (
        "data/raw/events.jsonl"
    )

    hbase_namespace: str = "ecommerce"
    hbase_alert_table: str = "realtime_alerts"

    hdfs_output_path: str = (
        "/data/raw/streaming"
    )

    def validate(self) -> None:
        """
        Valida os parâmetros do job.
        """

        if not self.job_name.strip():
            raise ValueError(
                "job_name não pode ser vazio."
            )

        if self.parallelism < 1:
            raise ValueError(
                "parallelism deve ser maior que zero."
            )

        if self.watermark_out_of_orderness_seconds < 0:
            raise ValueError(
                "watermark_out_of_orderness_seconds "
                "deve ser maior ou igual a zero."
            )

        if self.sliding_window_size_seconds <= 0:
            raise ValueError(
                "sliding_window_size_seconds "
                "deve ser maior que zero."
            )

        if self.sliding_window_slide_seconds <= 0:
            raise ValueError(
                "sliding_window_slide_seconds "
                "deve ser maior que zero."
            )

        if (
            self.sliding_window_slide_seconds
            > self.sliding_window_size_seconds
        ):
            raise ValueError(
                "O slide da janela não pode ser maior "
                "que o tamanho da janela."
            )

        if self.checkpoint_interval_ms <= 0:
            raise ValueError(
                "checkpoint_interval_ms "
                "deve ser maior que zero."
            )


class FlinkStreamingJob:

    def __init__(
        self,
        config: Optional[FlinkJobConfig] = None,
    ) -> None:
        self.config = config or FlinkJobConfig()

        self.config.validate()

        self._running = False

        self._pipeline: Optional[Any] = None

    @property
    def running(self) -> bool:
        return self._running

    @property
    def pipeline(self) -> Optional[Any]:

        return self._pipeline

    def build_pipeline(self) -> Any:

        self.config.validate()

        pipeline_definition: Dict[str, Any] = {
            "job_name": self.config.job_name,
            "parallelism": self.config.parallelism,
            "source": {
                "path": self.config.input_path,
            },
            "watermark": {
                "out_of_orderness_seconds": (
                    self.config
                    .watermark_out_of_orderness_seconds
                ),
            },
            "window": {
                "type": "sliding",
                "size_seconds": (
                    self.config
                    .sliding_window_size_seconds
                ),
                "slide_seconds": (
                    self.config
                    .sliding_window_slide_seconds
                ),
            },
            "checkpoint": {
                "interval_ms": (
                    self.config.checkpoint_interval_ms
                ),
            },
            "sinks": {
                "hbase": {
                    "namespace": self.config.hbase_namespace,
                    "table": self.config.hbase_alert_table,
                },
                "hdfs": {
                    "path": self.config.hdfs_output_path,
                },
            },
        }

        self._pipeline = pipeline_definition

        logger.info(
            "Pipeline Flink construída: %s",
            self.config.job_name,
        )

        return pipeline_definition

    def start(self) -> None:
        """
        Inicia o job de streaming.
        """

        if self._running:
            logger.warning(
                "O job Flink '%s' já está em execução.",
                self.config.job_name,
            )
            return

        if self._pipeline is None:
            self.build_pipeline()

        logger.info(
            "Iniciando job Flink '%s' com paralelismo %d.",
            self.config.job_name,
            self.config.parallelism,
        )

        self._running = True

    def stop(self) -> None:

        if not self._running:
            return

        logger.info(
            "Encerrando job Flink '%s'.",
            self.config.job_name,
        )

        self._running = False

    def healthcheck(self) -> bool:

        try:
            self.config.validate()
        except ValueError:
            return False

        return True


def create_flink_job(
    config: Optional[FlinkJobConfig] = None,
) -> FlinkStreamingJob:

    return FlinkStreamingJob(config=config)

def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "[%(name)s] %(message)s"
        ),
    )

    job = create_flink_job()

    if not job.healthcheck():
        raise RuntimeError(
            "Configuração inválida para o job Flink."
        )

    job.build_pipeline()

    logger.info(
        "Job '%s' pronto para execução.",
        job.config.job_name,
    )


if __name__ == "__main__":
    main()