from __future__ import annotations

import json
import logging
import signal
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .schemas import (
    CartEventSchema,
    ClickEventSchema,
    DeliveryEventSchema,
    EventSchema,
    OrderEventSchema,
)


logger = logging.getLogger(__name__)


EventGenerator = Callable[[], EventSchema]


class EventGeneratorService:

    def __init__(
        self,
        output_path: str | Path = "data/raw/events.jsonl",
        interval_seconds: float = 1.0,
        events_per_batch: int = 10,
        event_generators: Optional[List[EventGenerator]] = None,
    ) -> None:
        if interval_seconds < 0:
            raise ValueError("interval_seconds deve ser maior ou igual a zero.")

        if events_per_batch < 1:
            raise ValueError("events_per_batch deve ser maior que zero.")

        self.output_path = Path(output_path)
        self.interval_seconds = interval_seconds
        self.events_per_batch = events_per_batch

        self.event_generators: List[EventGenerator] = (
            event_generators or []
        )

        self._running = False
        self._events_generated = 0
        self._batches_generated = 0

    @property
    def running(self) -> bool:
        """Indica se o gerador está em execução."""
        return self._running

    @property
    def events_generated(self) -> int:
        """Retorna a quantidade total de eventos gerados."""
        return self._events_generated

    @property
    def batches_generated(self) -> int:
        """Retorna a quantidade total de lotes gerados."""
        return self._batches_generated

    def register_generator(
        self,
        generator: EventGenerator,
    ) -> None:
        """
        Registra um gerador especializado de eventos.
        """

        if not callable(generator):
            raise TypeError("generator deve ser uma função chamável.")

        self.event_generators.append(generator)

        logger.debug(
            "Gerador de eventos registrado: %s",
            getattr(generator, "__name__", repr(generator)),
        )

    def generate_event(self) -> EventSchema:
        """
        Gera um único evento utilizando os geradores registrados.
        """

        if not self.event_generators:
            raise RuntimeError(
                "Nenhum gerador de eventos foi registrado."
            )

        generator_index = (
            self._events_generated % len(self.event_generators)
        )

        generator = self.event_generators[generator_index]
        event = generator()

        if not isinstance(event, EventSchema):
            raise TypeError(
                "O gerador deve retornar uma instância de EventSchema."
            )

        return event

    def generate_batch(self) -> List[EventSchema]:
        """
        Gera um lote de eventos.
        """

        events: List[EventSchema] = []

        for _ in range(self.events_per_batch):
            event = self.generate_event()
            events.append(event)

        return events

    def serialize_event(
        self,
        event: EventSchema,
    ) -> str:
        """
        Serializa um evento para uma linha JSON.
        """

        if not isinstance(event, EventSchema):
            raise TypeError(
                "O objeto recebido deve ser uma instância de EventSchema."
            )

        return json.dumps(
            event.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def write_events(
        self,
        events: List[EventSchema],
    ) -> None:
        """
        Persiste eventos em formato JSON Lines.

        Cada evento ocupa uma única linha para permitir que o Apache
        Flume acompanhe o arquivo continuamente utilizando tail -F.
        """

        if not events:
            return

        self.output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with self.output_path.open(
            mode="a",
            encoding="utf-8",
        ) as output_file:
            for event in events:
                output_file.write(
                    self.serialize_event(event)
                )
                output_file.write("\n")

            output_file.flush()

        self._events_generated += len(events)
        self._batches_generated += 1

    def generate_once(self) -> List[EventSchema]:
        """
        Executa uma única iteração de geração e persistência.
        """

        events = self.generate_batch()
        self.write_events(events)

        logger.info(
            "Lote gerado com %d eventos. Total acumulado: %d.",
            len(events),
            self._events_generated,
        )

        return events

    def start(self) -> None:
        """
        Inicia a geração contínua de eventos.
        """

        if self._running:
            logger.warning("O gerador já está em execução.")
            return

        if not self.event_generators:
            raise RuntimeError(
                "Não é possível iniciar sem geradores registrados."
            )

        self._running = True

        logger.info(
            "Iniciando geração contínua de eventos: "
            "intervalo=%ss, eventos_por_lote=%d, saída=%s.",
            self.interval_seconds,
            self.events_per_batch,
            self.output_path,
        )

        try:
            while self._running:
                started_at = time.monotonic()

                self.generate_once()

                elapsed = time.monotonic() - started_at
                sleep_time = max(
                    0.0,
                    self.interval_seconds - elapsed,
                )

                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info(
                "Interrupção manual recebida. Encerrando gerador."
            )

        finally:
            self.stop()

    def stop(self) -> None:
        """
        Solicita encerramento controlado da geração.
        """

        if self._running:
            logger.info(
                "Encerrando gerador. Eventos produzidos: %d.",
                self._events_generated,
            )

        self._running = False

    def register_signal_handlers(self) -> None:
        """
        Registra sinais do sistema para encerramento controlado.
        """

        signal.signal(
            signal.SIGINT,
            self._handle_shutdown_signal,
        )

        signal.signal(
            signal.SIGTERM,
            self._handle_shutdown_signal,
        )

    def _handle_shutdown_signal(
        self,
        signum: int,
        _frame: Any,
    ) -> None:
        logger.info(
            "Sinal %d recebido. Solicitando encerramento.",
            signum,
        )

        self.stop()


def create_default_generator(
    output_path: str | Path = "data/raw/events.jsonl",
    interval_seconds: float = 1.0,
    events_per_batch: int = 10,
) -> EventGeneratorService:
    """
    Cria o serviço principal do gerador.

    Os geradores especializados serão registrados posteriormente
    conforme os módulos de click, cart, order e delivery forem
    implementados.
    """

    return EventGeneratorService(
        output_path=output_path,
        interval_seconds=interval_seconds,
        events_per_batch=events_per_batch,
    )


def main() -> None:
    """
    Ponto de entrada para execução direta do gerador.
    """

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "[%(name)s] %(message)s"
        ),
    )

    generator = create_default_generator()

    logger.info(
        "Gerador inicializado. "
        "Aguardando registro dos geradores especializados."
    )

    generator.register_signal_handlers()

    if not generator.event_generators:
        logger.error(
            "Nenhum gerador especializado foi registrado. "
            "A execução será encerrada."
        )
        return

    generator.start()


if __name__ == "__main__":
    main()