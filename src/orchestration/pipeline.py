from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from threading import Lock
from typing import Any, Callable

from .jobs.daily_batch_job import DailyBatchJob
from .jobs.streaming_job import StreamingJob


logger = logging.getLogger(__name__)


class PipelineStatus(StrEnum):

    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class PipelineRun:

    run_id: str
    status: PipelineStatus
    started_at: datetime
    finished_at: datetime | None = None
    streaming: dict[str, Any] = field(default_factory=dict)
    batch: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id não pode ser vazio.")

        if self.started_at.tzinfo is None:
            raise ValueError(
                "started_at deve possuir timezone."
            )

        if (
            self.finished_at is not None
            and self.finished_at.tzinfo is None
        ):
            raise ValueError(
                "finished_at deve possuir timezone."
            )

        if (
            self.finished_at is not None
            and self.finished_at < self.started_at
        ):
            raise ValueError(
                "finished_at não pode ser anterior a started_at."
            )

    @property
    def duration_seconds(self) -> float | None:
        """Retorna a duração da execução em segundos."""

        if self.finished_at is None:
            return None

        return (
            self.finished_at - self.started_at
        ).total_seconds()

    @property
    def is_finished(self) -> bool:
        """Indica se a execução já foi finalizada."""

        return self.status in {
            PipelineStatus.STOPPED,
            PipelineStatus.FAILED,
        }

    def to_dict(self) -> dict[str, Any]:

        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "finished_at": (
                self.finished_at.isoformat()
                if self.finished_at
                else None
            ),
            "duration_seconds": self.duration_seconds,
            "streaming": dict(self.streaming),
            "batch": dict(self.batch),
            "error": self.error,
        }


@dataclass(slots=True)
class PipelineOrchestratorConfig:

    run_id_prefix: str = "pipeline"
    enable_streaming: bool = True
    enable_batch: bool = True
    fail_fast: bool = True
    retain_runs: int = 20

    def __post_init__(self) -> None:
        if not self.run_id_prefix.strip():
            raise ValueError(
                "run_id_prefix não pode ser vazio."
            )

        if self.retain_runs < 1:
            raise ValueError(
                "retain_runs deve ser maior que zero."
            )

        if not self.enable_streaming and not self.enable_batch:
            raise ValueError(
                "Pelo menos um fluxo deve estar habilitado."
            )


class PipelineOrchestrator:

    def __init__(
        self,
        streaming_job: StreamingJob | None = None,
        batch_job: DailyBatchJob | None = None,
        config: PipelineOrchestratorConfig | None = None,
    ) -> None:
        self.config = config or PipelineOrchestratorConfig()
        self.streaming_job = streaming_job
        self.batch_job = batch_job

        self._status = PipelineStatus.IDLE
        self._runs: list[PipelineRun] = []
        self._lock = Lock()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _generate_run_id(self) -> str:
        timestamp = self._now().strftime("%Y%m%d%H%M%S%f")

        return (
            f"{self.config.run_id_prefix}-"
            f"{timestamp}"
        )

    def _set_status(self, status: PipelineStatus) -> None:
        with self._lock:
            self._status = status

    @property
    def status(self) -> PipelineStatus:

        with self._lock:
            return self._status

    def start_streaming(self) -> dict[str, Any]:
        """Inicia o job de streaming."""

        if not self.config.enable_streaming:
            return {
                "status": "disabled",
                "message": "Streaming está desabilitado.",
            }

        if self.streaming_job is None:
            raise RuntimeError(
                "StreamingJob não foi configurado."
            )

        self._set_status(PipelineStatus.STARTING)

        try:
            result = self.streaming_job.start()

            self._set_status(PipelineStatus.RUNNING)

            return result

        except Exception:
            self._set_status(PipelineStatus.FAILED)

            logger.exception(
                "Falha ao iniciar o job de streaming."
            )

            raise

    def start_batch(self) -> dict[str, Any]:

        if not self.config.enable_batch:
            return {
                "status": "disabled",
                "message": "Batch está desabilitado.",
            }

        if self.batch_job is None:
            raise RuntimeError(
                "DailyBatchJob não foi configurado."
            )

        try:
            result = self.batch_job.run()

            if self.status == PipelineStatus.IDLE:
                self._set_status(PipelineStatus.RUNNING)

            return result

        except Exception:
            self._set_status(PipelineStatus.FAILED)

            logger.exception(
                "Falha na execução do job batch."
            )

            raise

    def run(
        self,
        run_id: str | None = None,
        *,
        run_streaming: bool = True,
        run_batch: bool = True,
    ) -> PipelineRun:

        if not run_streaming and not run_batch:
            raise ValueError(
                "Pelo menos um fluxo deve ser selecionado."
            )

        effective_run_id = run_id or self._generate_run_id()
        started_at = self._now()

        self._set_status(PipelineStatus.STARTING)

        streaming_result: dict[str, Any] = {}
        batch_result: dict[str, Any] = {}

        try:
            if run_streaming and self.config.enable_streaming:
                streaming_result = self.start_streaming()

            if run_batch and self.config.enable_batch:
                batch_result = self.start_batch()

            finished_at = self._now()

            pipeline_run = PipelineRun(
                run_id=effective_run_id,
                status=self.status,
                started_at=started_at,
                finished_at=finished_at,
                streaming=streaming_result,
                batch=batch_result,
            )

            self._register_run(pipeline_run)

            return pipeline_run

        except Exception as exc:
            finished_at = self._now()

            pipeline_run = PipelineRun(
                run_id=effective_run_id,
                status=PipelineStatus.FAILED,
                started_at=started_at,
                finished_at=finished_at,
                streaming=streaming_result,
                batch=batch_result,
                error=str(exc),
            )

            self._register_run(pipeline_run)

            if self.config.fail_fast:
                raise

            return pipeline_run

    def stop_streaming(self) -> dict[str, Any]:

        if self.streaming_job is None:
            return {
                "status": "not_configured",
                "message": "StreamingJob não foi configurado.",
            }

        self._set_status(PipelineStatus.STOPPING)

        try:
            result = self.streaming_job.stop()

            self._set_status(PipelineStatus.STOPPED)

            return result

        except Exception:
            self._set_status(PipelineStatus.FAILED)

            logger.exception(
                "Falha ao interromper o job de streaming."
            )

            raise

    def stop(self) -> dict[str, Any]:

        self._set_status(PipelineStatus.STOPPING)

        results: dict[str, Any] = {}

        if self.streaming_job is not None:
            try:
                results["streaming"] = self.streaming_job.stop()
            except Exception as exc:
                results["streaming"] = {
                    "status": "failed",
                    "error": str(exc),
                }

        if self.batch_job is not None:
            try:
                results["batch"] = self.batch_job.stop()
            except Exception as exc:
                results["batch"] = {
                    "status": "failed",
                    "error": str(exc),
                }

        has_failure = any(
            result.get("status") == "failed"
            for result in results.values()
        )

        self._set_status(
            PipelineStatus.FAILED
            if has_failure
            else PipelineStatus.STOPPED
        )

        return {
            "status": self.status.value,
            "components": results,
        }

    def healthcheck(self) -> dict[str, Any]:

        checks: dict[str, Any] = {}

        if self.streaming_job is not None:
            checks["streaming"] = (
                self.streaming_job.healthcheck()
            )

        if self.batch_job is not None:
            checks["batch"] = self.batch_job.healthcheck()

        component_statuses = [
            result.get("status")
            for result in checks.values()
        ]

        healthy = bool(component_statuses) and all(
            status in {"healthy", "running", "ready"}
            for status in component_statuses
        )

        return {
            "status": "healthy" if healthy else "unhealthy",
            "pipeline_status": self.status.value,
            "components": checks,
        }

    def _register_run(self, pipeline_run: PipelineRun) -> None:
        with self._lock:
            self._runs.append(pipeline_run)

            if len(self._runs) > self.config.retain_runs:
                self._runs = self._runs[
                    -self.config.retain_runs:
                ]

    def last_run(self) -> PipelineRun | None:

        with self._lock:
            if not self._runs:
                return None

            return self._runs[-1]

    def runs(self) -> tuple[PipelineRun, ...]:

        with self._lock:
            return tuple(self._runs)

    def reset(self) -> None:

        with self._lock:
            self._runs.clear()
            self._status = PipelineStatus.IDLE

    def describe(self) -> dict[str, Any]:

        return {
            "status": self.status.value,
            "run_id_prefix": self.config.run_id_prefix,
            "streaming_enabled": self.config.enable_streaming,
            "batch_enabled": self.config.enable_batch,
            "fail_fast": self.config.fail_fast,
            "retained_runs": self.config.retain_runs,
            "streaming_configured": self.streaming_job is not None,
            "batch_configured": self.batch_job is not None,
            "run_count": len(self.runs()),
            "last_run": (
                self.last_run().to_dict()
                if self.last_run()
                else None
            ),
        }