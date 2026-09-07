from __future__ import annotations

import random
from decimal import Decimal
from typing import Sequence, TypeVar


T = TypeVar("T")


def random_choice(
    values: Sequence[T],
) -> T:
    """
    Seleciona aleatoriamente um elemento da sequência.
    """

    if not values:
        raise ValueError(
            "A sequência não pode estar vazia."
        )

    return random.choice(values)


def random_int(
    minimum: int,
    maximum: int,
) -> int:
    """
    Gera um número inteiro aleatório dentro do intervalo.
    """

    if minimum > maximum:
        raise ValueError(
            "minimum não pode ser maior que maximum."
        )

    return random.randint(
        minimum,
        maximum,
    )


def random_decimal(
    minimum: Decimal,
    maximum: Decimal,
    decimal_places: int = 2,
) -> Decimal:
    """
    Gera um valor decimal aleatório dentro do intervalo.
    """

    if minimum > maximum:
        raise ValueError(
            "minimum não pode ser maior que maximum."
        )

    if decimal_places < 0:
        raise ValueError(
            "decimal_places não pode ser negativo."
        )

    scale = 10 ** decimal_places

    minimum_scaled = int(
        minimum * scale
    )

    maximum_scaled = int(
        maximum * scale
    )

    value = random.randint(
        minimum_scaled,
        maximum_scaled,
    )

    return (
        Decimal(value) / Decimal(scale)
    ).quantize(
        Decimal(1).scaleb(-decimal_places)
    )


def random_probability(
    probability: float,
) -> bool:
    """
    Retorna True conforme a probabilidade informada.

    Exemplo:
        random_probability(0.15)

    significa aproximadamente 15% de chance de retornar True.
    """

    if not 0.0 <= probability <= 1.0:
        raise ValueError(
            "probability deve estar entre 0.0 e 1.0."
        )

    return random.random() < probability