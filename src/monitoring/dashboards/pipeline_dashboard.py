from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..alerts import AlertManager
from ..metrics import MetricsCollector
from ..pipeline_health import HealthStatus, PipelineHealth


@dataclass(frozen=True, slots=True)
class DashboardSnapshot:

    timestamp: datetime
    pipeline_status: HealthStatus
    health: dict[str, Any]
    metrics: dict[str, Any]
    alerts: dict[str, Any]

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError(
                "timestamp deve possuir timezone."
            )

    def to_dict(self) -> dict[str, Any]:

        return {
            "timestamp": self.timestamp.isoformat(),
            "pipeline_status": self.pipeline_status.value,
            "health": dict(self.health),
            "metrics": dict(self.metrics),
            "alerts": dict(self.alerts),
        }


class PipelineDashboard:

    def __init__(
        self,
        metrics: MetricsCollector,
        alerts: AlertManager,
        health: PipelineHealth,
    ) -> None:
        if metrics is None:
            raise ValueError(
                "metrics é obrigatório."
            )

        if alerts is None:
            raise ValueError(
                "alerts é obrigatório."
            )

        if health is None:
            raise ValueError(
                "health é obrigatório."
            )

        self._metrics = metrics
        self._alerts = alerts
        self._health = health

    @staticmethod
    def _now() -> datetime:
        """Retorna o horário atual em UTC."""

        return datetime.now(timezone.utc)

    def health_summary(self) -> dict[str, Any]:

        snapshot = self._health.snapshot()

        return {
            "status": snapshot["status"],
            "is_healthy": snapshot["is_healthy"],
            "is_available": snapshot["is_available"],
            "component_count": snapshot["component_count"],
            "status_counts": snapshot["status_counts"],
        }

    def metrics_summary(self) -> dict[str, Any]:

        snapshot = self._metrics.snapshot()

        values = {
            name: metric["value"]
            for name, metric in snapshot.items()
        }

        return {
            "metric_count": len(snapshot),
            "values": values,
            "metrics": snapshot,
        }

    def alerts_summary(self) -> dict[str, Any]:

        alerts = self._alerts.all()

        return {
            "total": len(alerts),
            "severity_counts": self._alerts.count_by_severity(),
            "critical": len(self._alerts.critical_alerts()),
            "warning": len(self._alerts.warning_alerts()),
            "types": sorted(
                {
                    alert.alert_type.value
                    for alert in alerts
                }
            ),
        }

    def processing_summary(self) -> dict[str, Any]:

        metric_names = (
            "events_received",
            "events_processed",
            "events_failed",
            "events_filtered",
            "events_late",
            "events_out_of_order",
            "processing_latency_ms",
            "ingestion_latency_ms",
        )

        return {
            name: self._metrics.get_value(name)
            for name in metric_names
        }

    def streaming_summary(self) -> dict[str, Any]:

        metric_names = (
            "streaming_events_received",
            "streaming_events_processed",
            "streaming_events_failed",
            "streaming_events_late",
            "streaming_windows_processed",
            "streaming_alerts_generated",
        )

        return {
            name: self._metrics.get_value(name)
            for name in metric_names
        }

    def batch_summary(self) -> dict[str, Any]:

        metric_names = (
            "batch_jobs_started",
            "batch_jobs_completed",
            "batch_jobs_failed",
            "batch_records_processed",
            "batch_processing_latency_ms",
        )

        return {
            name: self._metrics.get_value(name)
            for name in metric_names
        }

    def storage_summary(self) -> dict[str, Any]:

        metric_names = (
            "hdfs_read_records",
            "hdfs_written_records",
            "hbase_read_records",
            "hbase_written_records",
            "hive_read_records",
            "hive_written_records",
        )

        return {
            name: self._metrics.get_value(name)
            for name in metric_names
        }

    def component_status(self) -> list[dict[str, Any]]:

        return [
            health.to_dict()
            for health in self._health.all()
        ]

    def recent_alerts(
        self,
        limit: int = 10,
    ) -> list[dict[str, Any]]:

        if limit <= 0:
            raise ValueError(
                "limit deve ser maior que zero."
            )

        alerts = sorted(
            self._alerts.all(),
            key=lambda alert: alert.timestamp,
            reverse=True,
        )

        return [
            alert.to_dict()
            for alert in alerts[:limit]
        ]

    def snapshot(self) -> DashboardSnapshot:

        return DashboardSnapshot(
            timestamp=self._now(),
            pipeline_status=self._health.overall_status,
            health=self.health_summary(),
            metrics=self.metrics_summary(),
            alerts=self.alerts_summary(),
        )

    def dashboard_data(self) -> dict[str, Any]:

        return {
            "timestamp": self._now().isoformat(),
            "pipeline": {
                "status": self._health.overall_status.value,
                "is_healthy": self._health.is_healthy,
                "is_available": self._health.is_available,
            },
            "health": self.health_summary(),
            "metrics": self.metrics_summary(),
            "processing": self.processing_summary(),
            "streaming": self.streaming_summary(),
            "batch": self.batch_summary(),
            "storage": self.storage_summary(),
            "alerts": self.alerts_summary(),
            "components": self.component_status(),
            "recent_alerts": self.recent_alerts(),
        }

    def is_operational(self) -> bool:

        return self._health.is_available

    def describe(self) -> dict[str, Any]:

        return {
            "pipeline_status": self._health.overall_status.value,
            "is_operational": self.is_operational(),
            "health": self._health.describe(),
            "metrics": self._metrics.describe(),
            "alerts": self._alerts.describe(),
        }