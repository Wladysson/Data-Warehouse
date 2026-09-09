from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class Partition:

    year: int
    month: int
    day: int
    hour: int | None = None

    def __post_init__(self) -> None:
        if self.year < 1970:
            raise ValueError("O ano da partição deve ser válido.")

        if not 1 <= self.month <= 12:
            raise ValueError("O mês deve estar entre 1 e 12.")

        if not 1 <= self.day <= 31:
            raise ValueError("O dia deve estar entre 1 e 31.")

        if self.hour is not None and not 0 <= self.hour <= 23:
            raise ValueError("A hora deve estar entre 0 e 23.")

    @classmethod
    def from_date(cls, value: date) -> "Partition":

        return cls(
            year=value.year,
            month=value.month,
            day=value.day,
        )

    @classmethod
    def from_datetime(cls, value: datetime) -> "Partition":

        return cls(
            year=value.year,
            month=value.month,
            day=value.day,
            hour=value.hour,
        )

    def to_dict(self) -> dict[str, int]:

        values = {
            "year": self.year,
            "month": self.month,
            "day": self.day,
        }

        if self.hour is not None:
            values["hour"] = self.hour

        return values

    def to_path(self) -> str:

        path = (
            f"year={self.year:04d}"
            f"/month={self.month:02d}"
            f"/day={self.day:02d}"
        )

        if self.hour is not None:
            path = f"{path}/hour={self.hour:02d}"

        return path

    def to_spec(self) -> str:

        spec = (
            f"year={self.year}"
            f",month={self.month}"
            f",day={self.day}"
        )

        if self.hour is not None:
            spec = f"{spec},hour={self.hour}"

        return spec


@dataclass(frozen=True, slots=True)
class PartitionManager:

    partition_columns: tuple[str, ...] = (
        "year",
        "month",
        "day",
    )

    include_hour: bool = False

    def __post_init__(self) -> None:
        expected = ("year", "month", "day")

        if self.partition_columns[:3] != expected:
            raise ValueError(
                "As três primeiras colunas devem ser year, month e day."
            )

        if self.include_hour and "hour" not in self.partition_columns:
            raise ValueError(
                "A coluna hour deve estar presente quando include_hour=True."
            )

    def create(
        self,
        value: date | datetime,
    ) -> Partition:

        if isinstance(value, datetime):
            partition = Partition.from_datetime(value)

            if not self.include_hour:
                return Partition(
                    year=partition.year,
                    month=partition.month,
                    day=partition.day,
                )

            return partition

        return Partition.from_date(value)

    def create_range(
        self,
        start: date,
        end: date,
    ) -> list[Partition]:

        if end < start:
            raise ValueError(
                "A data final não pode ser anterior à data inicial."
            )

        partitions: list[Partition] = []
        current = start

        while current <= end:
            partitions.append(self.create(current))

            current = date.fromordinal(
                current.toordinal() + 1,
            )

        return partitions

    def path(
        self,
        base_path: str,
        value: date | datetime,
    ) -> str:

        normalized_base = base_path.rstrip("/")

        if not normalized_base:
            normalized_base = "/"

        partition = self.create(value)

        if normalized_base == "/":
            return f"/{partition.to_path()}"

        return f"{normalized_base}/{partition.to_path()}"

    def hive_spec(
        self,
        value: date | datetime,
    ) -> str:

        return self.create(value).to_spec()

    def sql_partition_spec(
        self,
        value: date | datetime,
    ) -> str:

        partition = self.create(value)

        values = [
            f"year={partition.year}",
            f"month={partition.month}",
            f"day={partition.day}",
        ]

        if partition.hour is not None:
            values.append(f"hour={partition.hour}")

        return ", ".join(values)

    def describe(self) -> dict[str, object]:

        return {
            "partition_columns": self.partition_columns,
            "include_hour": self.include_hour,
        }