from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

from src.ingestion.validators.event_validator import EventValidator
from src.streaming.models import StreamingEvent


logger = logging.getLogger(__name__)


class FileSource:

    def __init__(
        self,
        input_path: str | Path = "data/raw/events.jsonl",
        validator: Optional[EventValidator] = None,
    ) -> None:
        self.input_path = Path(input_path)
        self.validator = validator or EventValidator()

    def exists(self) -> bool:
        return self.input_path.is_file()

    def validate_source(self) -> None:
        if not self.exists():
            raise FileNotFoundError(
                f"Arquivo de eventos não encontrado: "
                f"{self.input_path}"
            )

    def read_lines(self) -> Iterator[str]:
        self.validate_source()

        with self.input_path.open(
            mode="r",
            encoding="utf-8",
        ) as input_file:
            for line in input_file:
                normalized_line = line.strip()

                if not normalized_line:
                    continue

                yield normalized_line

    def read_events(self) -> Iterator[Dict[str, Any]]:
        for line_number, payload in enumerate(
            self.read_lines(),
            start=1,
        ):
            try:
                event = json.loads(payload)

                if not isinstance(event, dict):
                    raise ValueError(
                        "O registro deve representar um objeto JSON."
                    )

                yield event

            except json.JSONDecodeError as exc:
                logger.warning(
                    "Registro JSON inválido na linha %d: %s",
                    line_number,
                    exc.msg,
                )

            except ValueError as exc:
                logger.warning(
                    "Registro inválido na linha %d: %s",
                    line_number,
                    exc,
                )

    def read_validated_events(
        self,
    ) -> Iterator[Dict[str, Any]]:
        for event in self.read_events():
            try:
                normalized_event = self.validator.normalize(
                    event
                )
                yield normalized_event

            except (TypeError, ValueError) as exc:
                logger.warning(
                    "Evento rejeitado durante validação: %s",
                    exc,
                )

    def to_streaming_event(
        self,
        event: Dict[str, Any],
    ) -> StreamingEvent:
        validated_event = self.validator.validate(event)

        return StreamingEvent(
            event_id=validated_event.event_id,
            event_type=validated_event.event_type,
            event_timestamp=validated_event.event_timestamp,
            ingestion_timestamp=(
                validated_event.ingestion_timestamp
            ),
            customer_id=validated_event.customer_id,
            session_id=validated_event.session_id,
            product_id=getattr(
                validated_event,
                "product_id",
                None,
            ),
            order_id=getattr(
                validated_event,
                "order_id",
                None,
            ),
            cart_id=getattr(
                validated_event,
                "cart_id",
                None,
            ),
            delivery_id=getattr(
                validated_event,
                "delivery_id",
                None,
            ),
            quantity=getattr(
                validated_event,
                "quantity",
                None,
            ),
            unit_price=getattr(
                validated_event,
                "unit_price",
                None,
            ),
            total_amount=getattr(
                validated_event,
                "total_amount",
                None,
            ),
            status=getattr(
                validated_event,
                "status",
                None,
            ),
            metadata=validated_event.metadata,
        )

    def stream(self) -> Iterator[StreamingEvent]:
        for event in self.read_validated_events():
            try:
                yield self.to_streaming_event(event)

            except (TypeError, ValueError) as exc:
                logger.warning(
                    "Falha ao converter evento para StreamingEvent: %s",
                    exc,
                )

    def count_events(self) -> int:
        return sum(
            1
            for _ in self.stream()
        )

    def healthcheck(self) -> bool:
        return self.exists()

    def describe(self) -> Dict[str, Any]:
        return {
            "type": "file",
            "input_path": str(self.input_path),
            "exists": self.exists(),
        }