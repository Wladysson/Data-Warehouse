from __future__ import annotations

import re
from collections.abc import Collection
from datetime import date, datetime
from typing import Any


class ValidationUtils:

    EMAIL_PATTERN = re.compile(
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    )

    IDENTIFIER_PATTERN = re.compile(
        r"^[A-Za-z_][A-Za-z0-9_]*$"
    )

    @staticmethod
    def required_string(
        value: Any,
        field_name: str,
    ) -> str:

        if not isinstance(value, str):
            raise TypeError(
                f"{field_name} deve ser uma string."
            )

        normalized = value.strip()

        if not normalized:
            raise ValueError(
                f"{field_name} não pode ser vazio."
            )

        return normalized

    @staticmethod
    def optional_string(
        value: Any,
        field_name: str,
    ) -> str | None:

        if value is None:
            return None

        return ValidationUtils.required_string(
            value,
            field_name,
        )

    @staticmethod
    def positive_integer(
        value: Any,
        field_name: str,
    ) -> int:

        if isinstance(value, bool) or not isinstance(
            value,
            int,
        ):
            raise TypeError(
                f"{field_name} deve ser um inteiro."
            )

        if value <= 0:
            raise ValueError(
                f"{field_name} deve ser maior que zero."
            )

        return value

    @staticmethod
    def non_negative_integer(
        value: Any,
        field_name: str,
    ) -> int:

        if isinstance(value, bool) or not isinstance(
            value,
            int,
        ):
            raise TypeError(
                f"{field_name} deve ser um inteiro."
            )

        if value < 0:
            raise ValueError(
                f"{field_name} não pode ser negativo."
            )

        return value

    @staticmethod
    def non_negative_number(
        value: Any,
        field_name: str,
    ) -> float:

        if isinstance(value, bool) or not isinstance(
            value,
            (int, float),
        ):
            raise TypeError(
                f"{field_name} deve ser numérico."
            )

        normalized = float(value)

        if normalized < 0:
            raise ValueError(
                f"{field_name} não pode ser negativo."
            )

        return normalized

    @staticmethod
    def email(
        value: Any,
        field_name: str = "email",
    ) -> str:

        normalized = ValidationUtils.required_string(
            value,
            field_name,
        ).lower()

        if not ValidationUtils.EMAIL_PATTERN.match(
            normalized
        ):
            raise ValueError(
                f"{field_name} possui formato inválido."
            )

        return normalized

    @staticmethod
    def identifier(
        value: Any,
        field_name: str = "identifier",
    ) -> str:

        normalized = ValidationUtils.required_string(
            value,
            field_name,
        )

        if not ValidationUtils.IDENTIFIER_PATTERN.match(
            normalized
        ):
            raise ValueError(
                f"{field_name} possui formato inválido."
            )

        return normalized

    @staticmethod
    def datetime_with_timezone(
        value: Any,
        field_name: str,
    ) -> datetime:

        if not isinstance(value, datetime):
            raise TypeError(
                f"{field_name} deve ser um datetime."
            )

        if value.tzinfo is None:
            raise ValueError(
                f"{field_name} deve possuir timezone."
            )

        return value

    @staticmethod
    def date(
        value: Any,
        field_name: str,
    ) -> date:

        if not isinstance(value, date):
            raise TypeError(
                f"{field_name} deve ser uma data."
            )

        return value

    @staticmethod
    def one_of(
        value: Any,
        allowed: Collection[Any],
        field_name: str,
    ) -> Any:

        if value not in allowed:
            raise ValueError(
                f"{field_name} deve ser um dos valores: "
                f"{', '.join(map(str, allowed))}."
            )

        return value

    @staticmethod
    def not_empty_collection(
        value: Any,
        field_name: str,
    ) -> Collection[Any]:

        if not isinstance(
            value,
            Collection,
        ):
            raise TypeError(
                f"{field_name} deve ser uma coleção."
            )

        if len(value) == 0:
            raise ValueError(
                f"{field_name} não pode ser vazia."
            )

        return value

    @staticmethod
    def range_value(
        value: float,
        minimum: float,
        maximum: float,
        field_name: str,
    ) -> float:

        if value < minimum or value > maximum:
            raise ValueError(
                f"{field_name} deve estar entre "
                f"{minimum} e {maximum}."
            )

        return value

    @staticmethod
    def same_length(
        first: Collection[Any],
        second: Collection[Any],
        first_name: str,
        second_name: str,
    ) -> bool:

        if len(first) != len(second):
            raise ValueError(
                f"{first_name} e {second_name} devem possuir "
                "o mesmo tamanho."
            )

        return True

    @staticmethod
    def temporal_order(
        start: datetime,
        end: datetime,
        start_name: str,
        end_name: str,
    ) -> bool:

        ValidationUtils.datetime_with_timezone(
            start,
            start_name,
        )

        ValidationUtils.datetime_with_timezone(
            end,
            end_name,
        )

        if end < start:
            raise ValueError(
                f"{end_name} não pode ser anterior a "
                f"{start_name}."
            )

        return True

    @staticmethod
    def approximately_equal(
        first: float,
        second: float,
        tolerance: float = 0.01,
    ) -> bool:

        if tolerance < 0:
            raise ValueError(
                "tolerance não pode ser negativa."
            )

        return abs(first - second) <= tolerance