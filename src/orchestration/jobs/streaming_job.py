from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ...streaming.flink_job import FlinkJobConfig, FlinkStreamingJob


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StreamingJobConfig:

    job_name: str = "ecommerce-streaming-job"
    flink: FlinkJobConfig = field(
        default_factory=FlinkJobConfig
    )
    auto_start: bool = False
    healthcheck_on_start: bool = True

    def __post_init__(self) -> None:
        if not self.job_name.strip():
            raise ValueError(
                "job_name não pode ser vazio."
            )


class StreamingJob:

    def __init__(
        self,
        config: StreamingJobConfig | None = None,
        flink_job: FlinkStreamingJob | None = None,
    ) -> None:
        self.config = config or StreamingJobConfig()
        self.flink_job = flink_job or FlinkStreamingJob(
            config=self.config.flink
        )

        self._started_at: datetime | None = None
        self._running = False

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @property
    def is_running(self) -> bool:

        return self._running

    @property
    def started_at(self) -> datetime | None:

        return self._started_at

    def build_pipeline(self) -> dict[str, Any]:

        return self.flink_job.build_pipeline()

    def start(self) -> dict[str, Any]:

        if self._running:
            return {
                "status": "running",
                "message": "Streaming já está em execução.",
                "started_at": (
                    self._started_at.isoformat()
                    if self._started_at
                    else None
                ),
            }

        try:
            pipeline = self.flink_job.build_pipeline()
            result = self.flink_job.start()

            self._started_at = self._now()
            self._running = True

            if self.config.healthcheck_on_start:
                health = self.healthcheck()
            else:
                health = {
                    "status": "not_checked",
                }

            return {
                "status": "running",
                "job_name": self.config.job_name,
                "started_at": self._started_at.isoformat(),
                "pipeline": pipeline,
                "start_result": result,
                "health": health,
            }

        except Exception:
            self._running = False

            logger.exception(
                "Falha ao iniciar o job de streaming."
            )

            raise

    def stop(self) -> dict[str, Any]:

        if not self._running:
            return {
                "status": "stopped",
                "message": "Streaming não está em execução.",
            }

        try:
            result = self.flink_job.stop()

            self._running = False

            return {
                "status": "stopped",
                "job_name": self.config.job_name,
                "stop_result": result,
            }

        except Exception:
            logger.exception(
                "Falha ao interromper o job de streaming."
            )

            raise

    def healthcheck(self) -> dict[str, Any]:

        result = self.flink_job.healthcheck()

        return {
            "status": (
                "healthy"
                if result
                else "unhealthy"
            ),
            "running": self._running,
            "job_name": self.config.job_name,
            "started_at": (
                self._started_at.isoformat()
                if self._started_at
                else None
            ),
            "flink": result,
        }

    def describe(self) -> dict[str, Any]:

        return {
            "job_name": self.config.job_name,
            "running": self._running,
            "auto_start": self.config.auto_start,
            "healthcheck_on_start": (
                self.config.healthcheck_on_start
            ),
            "started_at": (
                self._started_at.isoformat()
                if self._started_at
                else None
            ),
            "flink": self.flink_job.describe(),
        }