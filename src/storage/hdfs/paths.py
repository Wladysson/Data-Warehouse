from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HDFSPaths:

    base_path: str = "/data"

    @property
    def raw(self) -> str:

        return f"{self.base_path.rstrip('/')}/raw"

    @property
    def raw_events(self) -> str:

        return f"{self.raw}/events"

    @property
    def raw_clicks(self) -> str:

        return f"{self.raw}/clicks"

    @property
    def raw_carts(self) -> str:

        return f"{self.raw}/carts"

    @property
    def raw_orders(self) -> str:

        return f"{self.raw}/orders"

    @property
    def raw_deliveries(self) -> str:

        return f"{self.raw}/deliveries"

    @property
    def processed(self) -> str:

        return f"{self.base_path.rstrip('/')}/processed"

    @property
    def clean(self) -> str:

        return f"{self.processed}/clean"

    @property
    def curated(self) -> str:

        return f"{self.processed}/curated"

    @property
    def streaming(self) -> str:

        return f"{self.base_path.rstrip('/')}/streaming"

    @property
    def alerts(self) -> str:

        return f"{self.streaming}/alerts"

    @property
    def checkpoints(self) -> str:

        return f"{self.base_path.rstrip('/')}/checkpoints"

    @property
    def spark_checkpoints(self) -> str:

        return f"{self.checkpoints}/spark"

    @property
    def flink_checkpoints(self) -> str:

        return f"{self.checkpoints}/flink"

    @property
    def warehouse(self) -> str:

        return f"{self.base_path.rstrip('/')}/warehouse"

    def event_type_path(self, event_type: str) -> str:

        normalized = event_type.strip().lower()

        if not normalized:
            raise ValueError("O tipo de evento não pode ser vazio.")

        return f"{self.raw_events}/{normalized}"

    def partition_path(
        self,
        base_path: str,
        *,
        year: int,
        month: int,
        day: int,
        hour: int | None = None,
    ) -> str:

        if not 1 <= month <= 12:
            raise ValueError("O mês deve estar entre 1 e 12.")

        if not 1 <= day <= 31:
            raise ValueError("O dia deve estar entre 1 e 31.")

        if not 0 <= hour <= 23 if hour is not None else False:
            raise ValueError("A hora deve estar entre 0 e 23.")

        path = (
            f"{base_path.rstrip('/')}"
            f"/year={year:04d}"
            f"/month={month:02d}"
            f"/day={day:02d}"
        )

        if hour is not None:
            path = f"{path}/hour={hour:02d}"

        return path

    def describe(self) -> dict[str, str]:

        return {
            "base": self.base_path,
            "raw": self.raw,
            "raw_events": self.raw_events,
            "processed": self.processed,
            "clean": self.clean,
            "curated": self.curated,
            "streaming": self.streaming,
            "alerts": self.alerts,
            "checkpoints": self.checkpoints,
            "warehouse": self.warehouse,
        }