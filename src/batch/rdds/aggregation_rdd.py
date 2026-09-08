from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from pyspark import RDD


@dataclass(frozen=True, slots=True)
class AggregationRDDConfig:
    """Configuração das agregações baseadas em RDD."""

    event_type_field: str = "event_type"
    amount_field: str = "total_amount"
    quantity_field: str = "quantity"


class AggregationRDD:

    def __init__(
        self,
        config: AggregationRDDConfig | None = None,
    ) -> None:
        self.config = config or AggregationRDDConfig()

    @staticmethod
    def _validate_rdd(rdd: RDD) -> None:
        if not isinstance(rdd, RDD):
            raise TypeError(
                "rdd deve ser uma instância de pyspark.RDD."
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
        ).filter(
            lambda item: item[0] is not None
        )

    def count_by_key(
        self,
        pair_rdd: RDD,
    ) -> RDD:

        self._validate_rdd(pair_rdd)

        return pair_rdd.mapValues(
            lambda _: 1
        ).reduceByKey(
            lambda left, right: left + right
        )

    def sum_by_key(
        self,
        pair_rdd: RDD,
    ) -> RDD:

        self._validate_rdd(pair_rdd)

        return pair_rdd.reduceByKey(
            lambda left, right: left + right
        )

    def aggregate_sum(
        self,
        rdd: RDD,
        key_function: Callable[[dict[str, Any]], Any],
        value_function: Callable[[dict[str, Any]], float],
    ) -> RDD:

        self._validate_rdd(rdd)

        if not callable(key_function):
            raise TypeError(
                "key_function deve ser chamável."
            )

        if not callable(value_function):
            raise TypeError(
                "value_function deve ser chamável."
            )

        return (
            rdd.map(
                lambda event: (
                    key_function(event),
                    float(value_function(event)),
                )
            )
            .filter(
                lambda item: item[0] is not None
            )
            .reduceByKey(
                lambda left, right: left + right
            )
        )

    def aggregate_quantity(
        self,
        rdd: RDD,
    ) -> RDD:

        self._validate_rdd(rdd)

        event_type = self.config.event_type_field
        quantity = self.config.quantity_field

        return self.aggregate_sum(
            rdd,
            key_function=lambda event: event.get(
                event_type
            ),
            value_function=lambda event: event.get(
                quantity,
                0,
            ) or 0,
        )

    def aggregate_revenue_by_event_type(
        self,
        rdd: RDD,
    ) -> RDD:
        """Agrega receita por tipo de evento."""

        self._validate_rdd(rdd)

        event_type = self.config.event_type_field
        amount = self.config.amount_field

        return self.aggregate_sum(
            rdd,
            key_function=lambda event: event.get(
                event_type
            ),
            value_function=lambda event: event.get(
                amount,
                0,
            ) or 0,
        )

    def average_by_key(
        self,
        pair_rdd: RDD,
    ) -> RDD:

        self._validate_rdd(pair_rdd)

        zero_value = (0.0, 0)

        def seq_op(
            accumulator: tuple[float, int],
            value: float,
        ) -> tuple[float, int]:
            total, count = accumulator
            return total + float(value), count + 1

        def comb_op(
            left: tuple[float, int],
            right: tuple[float, int],
        ) -> tuple[float, int]:
            return (
                left[0] + right[0],
                left[1] + right[1],
            )

        return (
            pair_rdd
            .aggregateByKey(
                zero_value,
                seq_op,
                comb_op,
            )
            .mapValues(
                lambda result: (
                    result[0] / result[1]
                    if result[1] > 0
                    else 0.0
                )
            )
        )

    def calculate_sales_metrics(
        self,
        rdd: RDD,
    ) -> RDD:

        self._validate_rdd(rdd)

        event_type = self.config.event_type_field
        amount = self.config.amount_field
        quantity = self.config.quantity_field

        metrics = (
            rdd.map(
                lambda event: (
                    event.get(event_type),
                    (
                        1,
                        float(event.get(quantity, 0) or 0),
                        float(event.get(amount, 0) or 0),
                    ),
                )
            )
            .filter(
                lambda item: item[0] is not None
            )
            .reduceByKey(
                lambda left, right: (
                    left[0] + right[0],
                    left[1] + right[1],
                    left[2] + right[2],
                )
            )
        )

        return metrics.mapValues(
            lambda result: {
                "event_count": result[0],
                "total_quantity": result[1],
                "total_revenue": result[2],
                "average_revenue": (
                    result[2] / result[0]
                    if result[0] > 0
                    else 0.0
                ),
            }
        )

    def top_keys(
        self,
        pair_rdd: RDD,
        limit: int = 10,
    ) -> list[tuple[Any, Any]]:

        self._validate_rdd(pair_rdd)

        if limit < 1:
            raise ValueError(
                "limit deve ser maior que zero."
            )

        return pair_rdd.takeOrdered(
            limit,
            key=lambda item: -item[1],
        )

    def repartition_by_key(
        self,
        pair_rdd: RDD,
        num_partitions: int,
    ) -> RDD:
        """Reparticiona um PairRDD por chave."""

        self._validate_rdd(pair_rdd)

        if num_partitions < 1:
            raise ValueError(
                "num_partitions deve ser maior que zero."
            )

        return pair_rdd.partitionBy(
            num_partitions
        )

    def run(
        self,
        rdd: RDD,
    ) -> RDD:

        return self.calculate_sales_metrics(rdd)

    def describe(self) -> dict[str, str]:
        """Retorna a configuração da operação."""

        return {
            "operation": "aggregation_rdd",
            "event_type_field": self.config.event_type_field,
            "amount_field": self.config.amount_field,
            "quantity_field": self.config.quantity_field,
        }