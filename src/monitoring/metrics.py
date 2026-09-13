from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any


@dataclass(frozen=True, slots=True)
class Metric:

    name: str
    value: float
    timestamp: datetime
    unit: str = "count"
    tags: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError(
                "name não pode ser vazio."
            )

        if self.timestamp.tzinfo is None:
            raise ValueError(
                "timestamp deve possuir timezone."
            )

        if not self.unit.strip():
            raise ValueError(
                "unit não pode ser vazia."
            )

        if self.tags is not None:
            object.__setattr__(
                self,
                "tags",
                dict(self.tags),
            )

    def to_dict(self) -> dict[str, Any]:

        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "unit": self.unit,
            "tags": dict(self.tags or {}),
        }


class MetricsCollector:

    def __init__(self) -> None:
        self._metrics: dict[str, Metric] = {}
        self._counters: dict[str, float] = {}
        self._lock = Lock()

    @staticmethod
    def _now() -> datetime:

        return datetime.now(timezone.utc)

    @staticmethod
    def _validate_name(name: str) -> str:

        normalized = name.strip()

        if not normalized:
            raise ValueError(
                "name não pode ser vazio."
            )

        return normalized

    def record(
        self,
        name: str,
        value: float,
        unit: str = "count",
        tags: dict[str, str] | None = None,
    ) -> Metric:

        metric_name = self._validate_name(name)

        metric = Metric(
            name=metric_name,
            value=float(value),
            timestamp=self._now(),
            unit=unit,
            tags=tags,
        )

        with self._lock:
            self._metrics[metric_name] = metric

        return metric

    def increment(
        self,
        name: str,
        amount: float = 1.0,
        tags: dict[str, str] | None = None,
    ) -> Metric:

        metric_name = self._validate_name(name)

        with self._lock:
            current_value = self._counters.get(
                metric_name,
                0.0,
            )

            new_value = current_value + amount
            self._counters[metric_name] = new_value

        return self.record(
            name=metric_name,
            value=new_value,
            unit="count",
            tags=tags,
        )

    def decrement(
        self,
        name: str,
        amount: float = 1.0,
        tags: dict[str, str] | None = None,
    ) -> Metric:

        return self.increment(
            name=name,
            amount=-amount,
            tags=tags,
        )

    def set_value(
        self,
        name: str,
        value: float,
        unit: str = "count",
        tags: dict[str, str] | None = None,
    ) -> Metric:

        return self.record(
            name=name,
            value=value,
            unit=unit,
            tags=tags,
        )

    def observe_latency(
        self,
        name: str,
        milliseconds: float,
        tags: dict[str, str] | None = None,
    ) -> Metric:

        if milliseconds < 0:
            raise ValueError(
                "milliseconds não pode ser negativo."
            )

        return self.record(
            name=name,
            value=milliseconds,
            unit="milliseconds",
            tags=tags,
        )

    def get(self, name: str) -> Metric | None:

        metric_name = self._validate_name(name)

        with self._lock:
            return self._metrics.get(metric_name)

    def get_value(self, name: str) -> float | None:

        metric = self.get(name)

        if metric is None:
            return None

        return metric.value

    def all(self) -> list[Metric]:

        with self._lock:
            return list(self._metrics.values())

    def snapshot(self) -> dict[str, dict[str, Any]]:

        return {
            metric.name: metric.to_dict()
            for metric in self.all()
        }

    def reset(self) -> None:

        with self._lock:
            self._metrics.clear()
            self._counters.clear()

    def describe(self) -> dict[str, Any]:

        return {
            "metric_count": len(self._metrics),
            "counter_count": len(self._counters),
            "metric_names": sorted(self._metrics),
        }