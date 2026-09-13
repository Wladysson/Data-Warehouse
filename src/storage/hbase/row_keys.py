from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class RowKeyBuilder:

    separator: str = ":"

    def __post_init__(self) -> None:
        if not self.separator:
            raise ValueError(
                "O separador da row key não pode ser vazio."
            )

    def event(
        self,
        event_type: str,
        event_id: str,
    ) -> str:

        return self.build(
            event_type,
            event_id,
        )

    def alert(
        self,
        alert_type: str,
        event_id: str,
        timestamp: datetime | int | str,
    ) -> str:

        normalized_timestamp = self.timestamp(timestamp)

        return self.build(
            alert_type,
            event_id,
            normalized_timestamp,
        )

    def sales_metric(
        self,
        event_date: date | datetime | str,
        dimension: str,
        key: str,
    ) -> str:

        return self.build(
            self.date(event_date),
            dimension,
            key,
        )

    def delivery_metric(
        self,
        event_date: date | datetime | str,
        carrier: str,
        order_id: str,
    ) -> str:

        return self.build(
            self.date(event_date),
            carrier,
            order_id,
        )

    def customer(
        self,
        customer_id: str,
        event_timestamp: datetime | int | str | None = None,
    ) -> str:

        if event_timestamp is None:
            return self.build(
                "customer",
                customer_id,
            )

        return self.build(
            "customer",
            customer_id,
            self.timestamp(event_timestamp),
        )

    def product(
        self,
        product_id: str,
        event_timestamp: datetime | int | str | None = None,
    ) -> str:

        if event_timestamp is None:
            return self.build(
                "product",
                product_id,
            )

        return self.build(
            "product",
            product_id,
            self.timestamp(event_timestamp),
        )

    def build(self, *parts: object) -> str:

        if not parts:
            raise ValueError(
                "A row key deve possuir pelo menos um componente."
            )

        normalized_parts: list[str] = []

        for part in parts:
            value = str(part).strip()

            if not value:
                raise ValueError(
                    "Os componentes da row key não podem ser vazios."
                )

            value = value.replace(
                self.separator,
                "_",
            )

            normalized_parts.append(value)

        return self.separator.join(normalized_parts)

    @staticmethod
    def date(value: date | datetime | str) -> str:

        if isinstance(value, datetime):
            return value.strftime("%Y%m%d")

        if isinstance(value, date):
            return value.strftime("%Y%m%d")

        normalized = value.strip()

        if not normalized:
            raise ValueError("A data não pode ser vazia.")

        try:
            parsed = datetime.fromisoformat(
                normalized.replace("Z", "+00:00"),
            )
        except ValueError:
            parsed_date = date.fromisoformat(normalized)
            return parsed_date.strftime("%Y%m%d")

        return parsed.strftime("%Y%m%d")

    @staticmethod
    def timestamp(value: datetime | int | str) -> str:

        if isinstance(value, datetime):
            return value.strftime("%Y%m%d%H%M%S%f")

        if isinstance(value, int):
            return str(value)

        normalized = value.strip()

        if not normalized:
            raise ValueError("O timestamp não pode ser vazio.")

        try:
            parsed = datetime.fromisoformat(
                normalized.replace("Z", "+00:00"),
            )
        except ValueError:
            return normalized

        return parsed.strftime("%Y%m%d%H%M%S%f")

    def prefixed(
        self,
        prefix: str,
        *parts: object,
    ) -> str:

        normalized_prefix = prefix.strip()

        if not normalized_prefix:
            raise ValueError(
                "O prefixo da row key não pode ser vazio."
            )

        return self.build(
            normalized_prefix,
            *parts,
        )

    def describe(self) -> dict[str, str]:

        return {
            "separator": self.separator,
        }