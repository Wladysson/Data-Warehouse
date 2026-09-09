from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class HBaseTableDefinition:

    name: str
    description: str
    column_families: tuple[str, ...]
    row_key_pattern: str
    max_versions: int = 1

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("O nome da tabela não pode ser vazio.")

        if not self.column_families:
            raise ValueError(
                "A tabela deve possuir pelo menos uma família de colunas."
            )

        if not self.row_key_pattern.strip():
            raise ValueError("O padrão do row key deve ser informado.")

        if self.max_versions <= 0:
            raise ValueError(
                "O número máximo de versões deve ser maior que zero."
            )


class HBaseTable:

    ALERTS: ClassVar[HBaseTableDefinition] = HBaseTableDefinition(
        name="ecommerce_alerts",
        description="Alertas gerados pelo processamento de streaming do Flink.",
        column_families=("alert", "event", "metric"),
        row_key_pattern="alert_type:event_id:timestamp",
    )

    STREAMING_EVENTS: ClassVar[HBaseTableDefinition] = HBaseTableDefinition(
        name="ecommerce_streaming_events",
        description="Resultados de eventos processados em tempo real.",
        column_families=("event", "customer", "commerce", "logistics"),
        row_key_pattern="event_type:event_id",
    )

    SALES_METRICS: ClassVar[HBaseTableDefinition] = HBaseTableDefinition(
        name="ecommerce_sales_metrics",
        description="Métricas comerciais agregadas para consultas de baixa latência.",
        column_families=("sales", "customer", "product", "time"),
        row_key_pattern="date:dimension:key",
    )

    DELIVERY_METRICS: ClassVar[HBaseTableDefinition] = HBaseTableDefinition(
        name="ecommerce_delivery_metrics",
        description="Métricas operacionais e alertas de desempenho logístico.",
        column_families=("delivery", "order", "carrier", "time"),
        row_key_pattern="date:carrier:order_id",
    )

    @classmethod
    def all(cls) -> tuple[HBaseTableDefinition, ...]:

        return (
            cls.ALERTS,
            cls.STREAMING_EVENTS,
            cls.SALES_METRICS,
            cls.DELIVERY_METRICS,
        )

    @classmethod
    def names(cls) -> tuple[str, ...]:
        """Retorna os nomes físicos das tabelas."""

        return tuple(
            table.name
            for table in cls.all()
        )

    @classmethod
    def get(cls, name: str) -> HBaseTableDefinition:
        """Obtém uma tabela pelo nome."""

        normalized_name = name.strip().lower()

        for table in cls.all():
            if table.name == normalized_name:
                return table

        raise KeyError(
            f"Tabela HBase não encontrada: {name}"
        )

    @classmethod
    def describe(cls) -> dict[str, dict[str, object]]:

        return {
            table.name: {
                "description": table.description,
                "column_families": table.column_families,
                "row_key_pattern": table.row_key_pattern,
                "max_versions": table.max_versions,
            }
            for table in cls.all()
        }

    @classmethod
    def definitions_for_creation(
        cls,
    ) -> dict[str, dict[str, dict[str, int]]]:

        return {
            table.name: {
                f"{family}:": {
                    "VERSIONS": table.max_versions,
                }
                for family in table.column_families
            }
            for table in cls.all()
        }