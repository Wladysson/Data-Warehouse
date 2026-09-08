from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True, slots=True)
class EventRDDConfig:
    """Configuração das operações de eventos em RDD."""

    event_type_field: str = "event_type"
    event_id_field: str = "event_id"


class EventRDD:

    def __init__(
        self,
        spark: SparkSession,
        config: EventRDDConfig | None = None,
    ) -> None:
        if spark is None:
            raise ValueError("spark não pode ser None.")

        self.spark = spark
        self.config = config or EventRDDConfig()

    @staticmethod
    def _validate_rdd(rdd: RDD) -> None:
        if not isinstance(rdd, RDD):
            raise TypeError(
                "rdd deve ser uma instância de pyspark.RDD."
            )

    @staticmethod
    def _validate_dataframe(dataframe: DataFrame) -> None:
        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                "dataframe deve ser uma instância de "
                "pyspark.sql.DataFrame."
            )

    def from_dataframe(
        self,
        dataframe: DataFrame,
    ) -> RDD:
        """Converte um DataFrame Spark em RDD de dicionários."""

        self._validate_dataframe(dataframe)

        return dataframe.rdd.map(
            lambda row: row.asDict(recursive=True)
        )

    def from_json_records(
        self,
        path: str,
    ) -> RDD:

        if not path.strip():
            raise ValueError("path não pode ser vazio.")

        import json

        return (
            self.spark.sparkContext.textFile(path)
            .filter(lambda line: bool(line.strip()))
            .map(json.loads)
        )

    def filter_by_event_type(
        self,
        rdd: RDD,
        event_type: str,
    ) -> RDD:

        self._validate_rdd(rdd)

        if not event_type.strip():
            raise ValueError(
                "event_type não pode ser vazio."
            )

        field = self.config.event_type_field

        return rdd.filter(
            lambda event: event.get(field) == event_type
        )

    def filter_valid_events(
        self,
        rdd: RDD,
    ) -> RDD:

        self._validate_rdd(rdd)

        event_id = self.config.event_id_field

        return rdd.filter(
            lambda event: (
                event is not None
                and bool(event.get(event_id))
            )
        )

    def normalize_event(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any]:

        normalized = dict(event)

        event_type = normalized.get(
            self.config.event_type_field
        )

        if isinstance(event_type, str):
            normalized[
                self.config.event_type_field
            ] = event_type.strip().lower()

        event_id = normalized.get(
            self.config.event_id_field
        )

        if isinstance(event_id, str):
            normalized[
                self.config.event_id_field
            ] = event_id.strip()

        return normalized

    def map_normalize(
        self,
        rdd: RDD,
    ) -> RDD:

        self._validate_rdd(rdd)

        return rdd.map(self.normalize_event)

    def distinct_events(
        self,
        rdd: RDD,
    ) -> RDD:

        self._validate_rdd(rdd)

        event_id = self.config.event_id_field

        return (
            rdd.map(
                lambda event: (
                    event.get(event_id),
                    event,
                )
            )
            .filter(lambda item: item[0] is not None)
            .reduceByKey(lambda first, _: first)
            .values()
        )

    def key_by_event_type(
        self,
        rdd: RDD,
    ) -> RDD:

        self._validate_rdd(rdd)

        field = self.config.event_type_field

        return rdd.map(
            lambda event: (
                event.get(field),
                event,
            )
        )

    def key_by_customer(
        self,
        rdd: RDD,
    ) -> RDD:

        self._validate_rdd(rdd)

        return rdd.filter(
            lambda event: event.get("customer_id") is not None
        ).map(
            lambda event: (
                event.get("customer_id"),
                event,
            )
        )

    def map_values(
        self,
        pair_rdd: RDD,
        mapper: Callable[[Any], Any],
    ) -> RDD:

        self._validate_rdd(pair_rdd)

        if not callable(mapper):
            raise TypeError("mapper deve ser chamável.")

        return pair_rdd.mapValues(mapper)

    def repartition(
        self,
        rdd: RDD,
        num_partitions: int,
    ) -> RDD:

        self._validate_rdd(rdd)

        if num_partitions < 1:
            raise ValueError(
                "num_partitions deve ser maior que zero."
            )

        return rdd.repartition(num_partitions)

    def coalesce(
        self,
        rdd: RDD,
        num_partitions: int,
    ) -> RDD:

        self._validate_rdd(rdd)

        if num_partitions < 1:
            raise ValueError(
                "num_partitions deve ser maior que zero."
            )

        return rdd.coalesce(
            num_partitions,
            shuffle=False,
        )

    def cache(
        self,
        rdd: RDD,
    ) -> RDD:

        self._validate_rdd(rdd)

        return rdd.cache()

    def persist(
        self,
        rdd: RDD,
        storage_level=None,
    ) -> RDD:

        self._validate_rdd(rdd)

        if storage_level is None:
            return rdd.persist()

        return rdd.persist(storage_level)

    def count(
        self,
        rdd: RDD,
    ) -> int:

        self._validate_rdd(rdd)

        return rdd.count()

    def run(
        self,
        dataframe: DataFrame,
        event_type: str | None = None,
    ) -> RDD:

        rdd = self.from_dataframe(dataframe)
        rdd = self.filter_valid_events(rdd)
        rdd = self.map_normalize(rdd)

        if event_type is not None:
            rdd = self.filter_by_event_type(
                rdd,
                event_type,
            )

        return self.distinct_events(rdd)

    def describe(self) -> dict[str, str]:

        return {
            "operation": "event_rdd",
            "event_type_field": self.config.event_type_field,
            "event_id_field": self.config.event_id_field,
        }