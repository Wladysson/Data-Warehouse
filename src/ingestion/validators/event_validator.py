from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Type

from pydantic import ValidationError

from src.data_generator.schemas import (
    CartEventSchema,
    ClickEventSchema,
    DeliveryEventSchema,
    EventSchema,
    OrderEventSchema,
)


class EventValidator:
    """
    Responsável pela validação estrutural e semântica dos eventos
    antes de sua entrada no fluxo de processamento.

    O validador utiliza o event_type para selecionar o schema
    correspondente ao evento.
    """

    _SCHEMAS: Dict[str, Type[EventSchema]] = {
        "click": ClickEventSchema,
        "cart": CartEventSchema,
        "order": OrderEventSchema,
        "delivery": DeliveryEventSchema,
    }

    def validate(
        self,
        event: Dict[str, Any],
    ) -> EventSchema:
        """
        Valida um evento representado por um dicionário.

        Retorna uma instância tipada do schema correspondente.
        """

        if not isinstance(event, dict):
            raise TypeError(
                "O evento deve ser representado por um dicionário."
            )

        event_type = event.get("event_type")

        if not event_type:
            raise ValueError(
                "O campo event_type é obrigatório."
            )

        schema = self._SCHEMAS.get(event_type)

        if schema is None:
            raise ValueError(
                f"Tipo de evento não suportado: {event_type}"
            )

        return schema.model_validate(event)

    def validate_json(
        self,
        payload: str,
    ) -> EventSchema:
        """
        Valida um evento recebido como JSON.
        """

        if not isinstance(payload, str):
            raise TypeError(
                "O payload deve ser uma string JSON."
            )

        payload = payload.strip()

        if not payload:
            raise ValueError(
                "O payload JSON não pode estar vazio."
            )

        try:
            event = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Payload JSON inválido: {exc.msg}"
            ) from exc

        return self.validate(event)

    def is_valid(
        self,
        event: Dict[str, Any],
    ) -> bool:
        """
        Verifica se um evento é válido sem propagar a exceção.
        """

        try:
            self.validate(event)
        except (
            TypeError,
            ValueError,
            ValidationError,
        ):
            return False

        return True

    def is_valid_json(
        self,
        payload: str,
    ) -> bool:
        """
        Verifica se um payload JSON é válido.
        """

        try:
            self.validate_json(payload)
        except (
            TypeError,
            ValueError,
            ValidationError,
        ):
            return False

        return True

    def validate_timestamp_order(
        self,
        event: EventSchema,
    ) -> bool:
        """
        Verifica se o timestamp de ocorrência não é posterior
        ao timestamp de ingestão.

        Eventos atrasados continuam sendo aceitos quando o
        event_timestamp é anterior ao ingestion_timestamp.
        """

        return (
            event.event_timestamp
            <= event.ingestion_timestamp
        )

    def validate_required_timestamps(
        self,
        event: EventSchema,
    ) -> bool:
        """
        Verifica se os timestamps possuem timezone definido.

        A pipeline trabalha exclusivamente com UTC.
        """

        return (
            event.event_timestamp.tzinfo is not None
            and event.ingestion_timestamp.tzinfo is not None
        )

    def validate_temporal_consistency(
        self,
        event: EventSchema,
    ) -> bool:
        """
        Executa as validações temporais do evento.
        """

        if not self.validate_required_timestamps(event):
            return False

        return self.validate_timestamp_order(event)

    def normalize(
        self,
        event: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Valida e normaliza um evento para representação JSON.
        """

        validated_event = self.validate(event)

        return validated_event.model_dump(
            mode="json"
        )

    def normalize_json(
        self,
        payload: str,
    ) -> str:
        """
        Valida, normaliza e serializa novamente um evento JSON.
        """

        normalized_event = self.validate_json(payload)

        return json.dumps(
            normalized_event.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
        )