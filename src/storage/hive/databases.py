from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class HiveDatabaseDefinition:

    name: str
    description: str
    location: str

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("O nome do banco não pode ser vazio.")

        if not self.location.strip():
            raise ValueError(
                "A localização do banco não pode ser vazia."
            )


class HiveDatabase:

    ECOMMERCE: ClassVar[HiveDatabaseDefinition] = HiveDatabaseDefinition(
        name="ecommerce",
        description=(
            "Data Warehouse consolidado da plataforma de e-commerce."
        ),
        location="hdfs://localhost:9000/data/warehouse/ecommerce",
    )

    ECOMMERCE_STAGING: ClassVar[HiveDatabaseDefinition] = (
        HiveDatabaseDefinition(
            name="ecommerce_staging",
            description=(
                "Área intermediária para dados preparados pelo "
                "processamento batch."
            ),
            location=(
                "hdfs://localhost:9000/data/warehouse/"
                "ecommerce_staging"
            ),
        )
    )

    ECOMMERCE_ANALYTICS: ClassVar[HiveDatabaseDefinition] = (
        HiveDatabaseDefinition(
            name="ecommerce_analytics",
            description=(
                "Camada analítica para consultas consolidadas "
                "e indicadores de negócio."
            ),
            location=(
                "hdfs://localhost:9000/data/warehouse/"
                "ecommerce_analytics"
            ),
        )
    )

    @classmethod
    def all(cls) -> tuple[HiveDatabaseDefinition, ...]:

        return (
            cls.ECOMMERCE,
            cls.ECOMMERCE_STAGING,
            cls.ECOMMERCE_ANALYTICS,
        )

    @classmethod
    def names(cls) -> tuple[str, ...]:

        return tuple(
            database.name
            for database in cls.all()
        )

    @classmethod
    def get(cls, name: str) -> HiveDatabaseDefinition:

        normalized_name = name.strip().lower()

        for database in cls.all():
            if database.name == normalized_name:
                return database

        raise KeyError(
            f"Banco Hive não encontrado: {name}"
        )

    @classmethod
    def describe(cls) -> dict[str, dict[str, str]]:

        return {
            database.name: {
                "description": database.description,
                "location": database.location,
            }
            for database in cls.all()
        }