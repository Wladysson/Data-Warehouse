from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from src.domain.enums.alert_types import AlertType


@dataclass(frozen=True, slots=True)
class Alert:

    alert_id: str
    alert_type: AlertType
    message: str
    severity: str
    timestamp: datetime
    source: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.alert_id.strip():
            raise ValueError(
                "alert_id não pode ser vazio."
            )

        if not self.message.strip():
            raise ValueError(
                "message não pode ser vazia."
            )

        if not self.severity.strip():
            raise ValueError(
                "severity não pode ser vazia."
            )

        if self.severity.lower() not in {
            "info",
            "warning",
            "critical",
        }:
            raise ValueError(
                "severity deve ser info, warning ou critical."
            )

        if not self.timestamp.tzinfo:
            raise ValueError(
                "timestamp deve possuir timezone."
            )

        if not self.source.strip():
            raise ValueError(
                "source não pode ser vazio."
            )

        object.__setattr__(
            self,
            "metadata",
            dict(self.metadata),
        )

    @property
    def is_critical(self) -> bool:

        return self.severity.lower() == "critical"

    @property
    def is_warning(self) -> bool:

        return self.severity.lower() == "warning"

    def to_dict(self) -> dict[str, Any]:

        return {
            "alert_id": self.alert_id,
            "alert_type": self.alert_type.value,
            "message": self.message,
            "severity": self.severity,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "metadata": dict(self.metadata),
        }


class AlertManager:

    def __init__(self) -> None:
        self._alerts: dict[str, Alert] = {}

    @staticmethod
    def _now() -> datetime:
        """Retorna o horário atual em UTC."""

        return datetime.now(timezone.utc)

    def create(
        self,
        alert_id: str,
        alert_type: AlertType,
        message: str,
        severity: str,
        source: str,
        metadata: dict[str, Any] | None = None,
    ) -> Alert:

        if alert_id in self._alerts:
            raise ValueError(
                f"Alerta '{alert_id}' já está registrado."
            )

        alert = Alert(
            alert_id=alert_id,
            alert_type=alert_type,
            message=message,
            severity=severity,
            timestamp=self._now(),
            source=source,
            metadata=metadata or {},
        )

        self._alerts[alert.alert_id] = alert

        return alert

    def get(self, alert_id: str) -> Alert | None:

        return self._alerts.get(alert_id)

    def all(self) -> list[Alert]:

        return list(self._alerts.values())

    def filter_by_type(
        self,
        alert_type: AlertType,
    ) -> list[Alert]:

        return [
            alert
            for alert in self._alerts.values()
            if alert.alert_type == alert_type
        ]

    def filter_by_severity(
        self,
        severity: str,
    ) -> list[Alert]:

        normalized = severity.strip().lower()

        if normalized not in {
            "info",
            "warning",
            "critical",
        }:
            raise ValueError(
                "severity deve ser info, warning ou critical."
            )

        return [
            alert
            for alert in self._alerts.values()
            if alert.severity.lower() == normalized
        ]

    def critical_alerts(self) -> list[Alert]:

        return self.filter_by_severity("critical")

    def warning_alerts(self) -> list[Alert]:

        return self.filter_by_severity("warning")

    def count(self) -> int:

        return len(self._alerts)

    def count_by_severity(self) -> dict[str, int]:

        result = {
            "info": 0,
            "warning": 0,
            "critical": 0,
        }

        for alert in self._alerts.values():
            result[alert.severity.lower()] += 1

        return result

    def snapshot(self) -> list[dict[str, Any]]:

        return [
            alert.to_dict()
            for alert in self._alerts.values()
        ]

    def clear(self) -> None:

        self._alerts.clear()

    def describe(self) -> dict[str, Any]:

        return {
            "alert_count": self.count(),
            "severity_counts": self.count_by_severity(),
            "alert_types": sorted(
                {
                    alert.alert_type.value
                    for alert in self._alerts.values()
                }
            ),
        }