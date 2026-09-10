from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


class HealthStatus(StrEnum):

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

@dataclass(frozen=True, slots=True)
class ComponentHealth:

    component: str
    status: HealthStatus
    timestamp: datetime
    message: str = ""
    latency_ms: float | None = None
    details: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.component.strip():
            raise ValueError(
                "component não pode ser vazio."
            )

        if self.timestamp.tzinfo is None:
            raise ValueError(
                "timestamp deve possuir timezone."
            )

        if self.latency_ms is not None and self.latency_ms < 0:
            raise ValueError(
                "latency_ms não pode ser negativo."
            )

        object.__setattr__(
            self,
            "details",
            dict(self.details or {}),
        )

    @property
    def is_healthy(self) -> bool:

        return self.status is HealthStatus.HEALTHY

    @property
    def is_available(self) -> bool:

        return self.status in {
            HealthStatus.HEALTHY,
            HealthStatus.DEGRADED,
        }

    def to_dict(self) -> dict[str, Any]:

        return {
            "component": self.component,
            "status": self.status.value,
            "timestamp": self.timestamp.isoformat(),
            "message": self.message,
            "latency_ms": self.latency_ms,
            "details": dict(self.details or {}),
        }


class PipelineHealth:

    def __init__(
        self,
        components: list[str] | tuple[str, ...] | None = None,
    ) -> None:
        self._components: dict[str, ComponentHealth] = {}

        for component in components or ():
            self.register(component)

    @staticmethod
    def _now() -> datetime:

        return datetime.now(timezone.utc)

    @staticmethod
    def _validate_component(component: str) -> str:

        normalized = component.strip()

        if not normalized:
            raise ValueError(
                "component não pode ser vazio."
            )

        return normalized

    def register(self, component: str) -> None:

        component_name = self._validate_component(component)

        if component_name not in self._components:
            self._components[component_name] = ComponentHealth(
                component=component_name,
                status=HealthStatus.UNKNOWN,
                timestamp=self._now(),
                message="Healthcheck ainda não executado.",
            )

    def update(
        self,
        component: str,
        status: HealthStatus,
        message: str = "",
        latency_ms: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> ComponentHealth:

        component_name = self._validate_component(component)

        self.register(component_name)

        health = ComponentHealth(
            component=component_name,
            status=status,
            timestamp=self._now(),
            message=message,
            latency_ms=latency_ms,
            details=details,
        )

        self._components[component_name] = health

        return health

    def mark_healthy(
        self,
        component: str,
        message: str = "Componente saudável.",
        latency_ms: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> ComponentHealth:

        return self.update(
            component=component,
            status=HealthStatus.HEALTHY,
            message=message,
            latency_ms=latency_ms,
            details=details,
        )

    def mark_degraded(
        self,
        component: str,
        message: str = "Componente operando com degradação.",
        latency_ms: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> ComponentHealth:

        return self.update(
            component=component,
            status=HealthStatus.DEGRADED,
            message=message,
            latency_ms=latency_ms,
            details=details,
        )

    def mark_unhealthy(
        self,
        component: str,
        message: str = "Componente indisponível.",
        latency_ms: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> ComponentHealth:

        return self.update(
            component=component,
            status=HealthStatus.UNHEALTHY,
            message=message,
            latency_ms=latency_ms,
            details=details,
        )

    def get(self, component: str) -> ComponentHealth | None:

        component_name = self._validate_component(component)

        return self._components.get(component_name)

    def all(self) -> list[ComponentHealth]:

        return list(self._components.values())

    def healthy_components(self) -> list[ComponentHealth]:

        return [
            health
            for health in self._components.values()
            if health.status is HealthStatus.HEALTHY
        ]

    def degraded_components(self) -> list[ComponentHealth]:

        return [
            health
            for health in self._components.values()
            if health.status is HealthStatus.DEGRADED
        ]

    def unhealthy_components(self) -> list[ComponentHealth]:

        return [
            health
            for health in self._components.values()
            if health.status is HealthStatus.UNHEALTHY
        ]

    @property
    def overall_status(self) -> HealthStatus:

        statuses = {
            health.status
            for health in self._components.values()
        }

        if not statuses:
            return HealthStatus.UNKNOWN

        if HealthStatus.UNHEALTHY in statuses:
            return HealthStatus.UNHEALTHY

        if HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED

        if HealthStatus.UNKNOWN in statuses:
            return HealthStatus.UNKNOWN

        return HealthStatus.HEALTHY

    @property
    def is_healthy(self) -> bool:

        return self.overall_status is HealthStatus.HEALTHY

    @property
    def is_available(self) -> bool:

        return self.overall_status in {
            HealthStatus.HEALTHY,
            HealthStatus.DEGRADED,
        }

    def status_counts(self) -> dict[str, int]:

        result = {
            status.value: 0
            for status in HealthStatus
        }

        for health in self._components.values():
            result[health.status.value] += 1

        return result

    def snapshot(self) -> dict[str, Any]:

        return {
            "status": self.overall_status.value,
            "is_healthy": self.is_healthy,
            "is_available": self.is_available,
            "component_count": len(self._components),
            "status_counts": self.status_counts(),
            "components": {
                health.component: health.to_dict()
                for health in self._components.values()
            },
            "timestamp": self._now().isoformat(),
        }

    def reset(self) -> None:

        self._components.clear()

    def describe(self) -> dict[str, Any]:

        return {
            "overall_status": self.overall_status.value,
            "component_count": len(self._components),
            "healthy_count": len(self.healthy_components()),
            "degraded_count": len(self.degraded_components()),
            "unhealthy_count": len(self.unhealthy_components()),
            "unknown_count": sum(
                1
                for health in self._components.values()
                if health.status is HealthStatus.UNKNOWN
            ),
            "components": sorted(self._components),
        }