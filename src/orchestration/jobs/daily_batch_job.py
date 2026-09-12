from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ...batch.spark_job import SparkBatchJob, SparkBatchJobConfig


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DailyBatchJobConfig:

    job_name: str = "ecommerce-daily-batch"
    spark: SparkBatchJobConfig | None = None
    execution_date: date | None = None
    fail_on_empty_input: bool = False

    def __post_init__(self) -> None:
        if not self.job_name.strip():
            raise ValueError(
                "job_name não pode ser vazio."
            )


class DailyBatchJob:
    
    def __init__(
        self,
        config: DailyBatchJobConfig | None = None,
        spark_job: SparkBatchJob | None = None,
    ) -> None:
        self.config = config or DailyBatchJobConfig()

        self.spark_job = spark_job or SparkBatchJob(
            config=self.config.spark
        )

        self._last_run_at: datetime | None = None
        self._last_result: dict[str, Any] | None = None

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @property
    def last_run_at(self) -> datetime | None:

        return self._last_run_at

    @property
    def last_result(self) -> dict[str, Any] | None:

        if self._last_result is None:
            return None

        return dict(self._last_result)

    def resolve_execution_date(
        self,
        execution_date: date | None = None,
    ) -> date:

        if execution_date is not None:
            return execution_date

        if self.config.execution_date is not None:
            return self.config.execution_date

        return self._now().date()

    def start(self) -> dict[str, Any]:

        session = self.spark_job.create_session()

        return {
            "status": "ready",
            "job_name": self.config.job_name,
            "spark_session": session is not None,
        }

    def run(
        self,
        execution_date: date | None = None,
    ) -> dict[str, Any]:

        processing_date = self.resolve_execution_date(
            execution_date
        )

        started_at = self._now()

        try:
            result = self.spark_job.run_sql_pipeline()

            finished_at = self._now()

            execution_result = {
                "status": "completed",
                "job_name": self.config.job_name,
                "execution_date": (
                    processing_date.isoformat()
                ),
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "duration_seconds": (
                    finished_at - started_at
                ).total_seconds(),
                "result": result,
            }

            self._last_run_at = finished_at
            self._last_result = execution_result

            return execution_result

        except Exception as exc:
            finished_at = self._now()

            execution_result = {
                "status": "failed",
                "job_name": self.config.job_name,
                "execution_date": (
                    processing_date.isoformat()
                ),
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "duration_seconds": (
                    finished_at - started_at
                ).total_seconds(),
                "error": str(exc),
            }

            self._last_run_at = finished_at
            self._last_result = execution_result

            logger.exception(
                "Falha na execução do batch diário para %s.",
                processing_date,
            )

            raise

    def stop(self) -> dict[str, Any]:

        try:
            self.spark_job.stop()

            return {
                "status": "stopped",
                "job_name": self.config.job_name,
            }

        except Exception:
            logger.exception(
                "Falha ao finalizar o job batch."
            )

            raise

    def healthcheck(self) -> dict[str, Any]:

        result = self.spark_job.healthcheck()

        return {
            "status": (
                "healthy"
                if result
                else "unhealthy"
            ),
            "job_name": self.config.job_name,
            "last_run_at": (
                self._last_run_at.isoformat()
                if self._last_run_at
                else None
            ),
            "spark": result,
        }

    def describe(self) -> dict[str, Any]:

        return {
            "job_name": self.config.job_name,
            "execution_date": (
                self.config.execution_date.isoformat()
                if self.config.execution_date
                else None
            ),
            "fail_on_empty_input": (
                self.config.fail_on_empty_input
            ),
            "last_run_at": (
                self._last_run_at.isoformat()
                if self._last_run_at
                else None
            ),
            "last_result": self.last_result,
            "spark": self.spark_job.describe(),
        }