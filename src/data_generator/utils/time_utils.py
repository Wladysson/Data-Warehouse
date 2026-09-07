from __future__ import annotations

from datetime import datetime, timedelta, timezone


def utc_now() -> datetime:
    """
    Retorna o horário atual em UTC.
    """

    return datetime.now(timezone.utc)


def generate_event_timestamp(
    delay_seconds: int = 0,
) -> datetime:
    """
    Gera um timestamp de evento em UTC.

    O atraso permite simular eventos cujo horário de ocorrência
    é anterior ao momento em que foram gerados.
    """

    if delay_seconds < 0:
        raise ValueError(
            "delay_seconds deve ser maior ou igual a zero."
        )

    return utc_now() - timedelta(
        seconds=delay_seconds,
    )


def generate_ingestion_timestamp() -> datetime:
    """
    Gera o timestamp correspondente ao momento de ingestão.
    """

    return utc_now()


def generate_out_of_order_timestamp(
    max_delay_seconds: int = 10,
) -> datetime:
    """
    Gera um timestamp de evento atrasado para simulação
    de eventos fora de ordem.

    O atraso é limitado ao intervalo informado.
    """

    if max_delay_seconds < 1:
        raise ValueError(
            "max_delay_seconds deve ser maior que zero."
        )

    return generate_event_timestamp(
        delay_seconds=max_delay_seconds,
    )