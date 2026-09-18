from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
from pyflink.datastream import DataStream

from pyflink.common import Duration, Types
from pyflink.common.watermark_strategy import (
    TimestampAssigner as FlinkTimestampAssigner,
)
from pyflink.common.watermark_strategy import (
    WatermarkStrategy as FlinkWatermarkStrategy,
)
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
)
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import (
    SlidingEventTimeWindows,
    Time,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FlinkJobConfig:

    job_name: str = "ecommerce-streaming-job"
    parallelism: int = 1

    watermark_out_of_orderness_seconds: int = 10

    sliding_window_size_seconds: int = 60
    sliding_window_slide_seconds: int = 10

    checkpoint_interval_ms: int = 60_000

    flink_socket_host: str = "0.0.0.0"
    flink_socket_port: int = 9999

    hbase_namespace: str = "ecommerce"
    hbase_alert_table: str = "realtime_alerts"

    hdfs_output_path: str = "/data/raw/streaming"

    def validate(self) -> None:

        if not self.job_name.strip():
            raise ValueError(
                "job_name não pode ser vazio."
            )

        if self.parallelism < 1:
            raise ValueError(
                "parallelism deve ser maior que zero."
            )

        if self.watermark_out_of_orderness_seconds < 0:
            raise ValueError(
                "watermark_out_of_orderness_seconds "
                "deve ser maior ou igual a zero."
            )

        if self.sliding_window_size_seconds <= 0:
            raise ValueError(
                "sliding_window_size_seconds "
                "deve ser maior que zero."
            )

        if self.sliding_window_slide_seconds <= 0:
            raise ValueError(
                "sliding_window_slide_seconds "
                "deve ser maior que zero."
            )

        if (
            self.sliding_window_slide_seconds
            > self.sliding_window_size_seconds
        ):
            raise ValueError(
                "O slide da janela não pode ser maior "
                "que o tamanho da janela."
            )

        if self.checkpoint_interval_ms <= 0:
            raise ValueError(
                "checkpoint_interval_ms "
                "deve ser maior que zero."
            )

        if not self.flink_socket_host.strip():
            raise ValueError(
                "flink_socket_host não pode ser vazio."
            )

        if not 1 <= self.flink_socket_port <= 65535:
            raise ValueError(
                "flink_socket_port deve estar entre 1 e 65535."
            )


class EventTimestampAssigner(
    FlinkTimestampAssigner
):

    def extract_timestamp(
        self,
        value: Dict[str, Any],
        record_timestamp: int,
    ) -> int:

        timestamp = value.get(
            "event_timestamp"
        )

        if not timestamp:
            return record_timestamp

        normalized_timestamp = (
            str(timestamp).replace(
                "Z",
                "+00:00",
            )
        )

        event_datetime = (
            datetime.fromisoformat(
                normalized_timestamp
            )
        )

        return int(
            event_datetime.timestamp() * 1000
        )


class WindowAggregationFunction(
    ProcessWindowFunction
):

    def process(
        self,
        key: str,
        context,
        elements,
    ):

        events = list(elements)

        if not events:
            return

        event_count = len(events)

        unique_customers = len(
            {
                event.get("customer_id")
                for event in events
                if event.get("customer_id")
            }
        )

        total_quantity = sum(
            int(
                event.get("quantity") or 0
            )
            for event in events
        )

        total_amount = sum(
            float(
                event.get("total_amount") or 0
            )
            for event in events
        )

        event_types = sorted(
            {
                event.get("event_type")
                for event in events
                if event.get("event_type")
            }
        )

        result = {
            "window_start": (
                context.window().start
            ),
            "window_end": (
                context.window().end
            ),
            "customer_id": key,
            "event_count": event_count,
            "unique_customers": unique_customers,
            "total_quantity": total_quantity,
            "total_amount": round(
                total_amount,
                2,
            ),
            "event_types": event_types,
        }

        yield json.dumps(
            result,
            ensure_ascii=False,
        )


class FlinkStreamingJob:

    def __init__(
        self,
        config: Optional[FlinkJobConfig] = None,
    ) -> None:

        self.config = (
            config or FlinkJobConfig()
        )

        self.config.validate()

        self._running = False

        self._pipeline: Optional[Any] = None

    @property
    def running(self) -> bool:
        return self._running

    @property
    def pipeline(self) -> Optional[Any]:
        return self._pipeline

    def _create_environment(
        self,
    ) -> StreamExecutionEnvironment:

        env = (
            StreamExecutionEnvironment
            .get_execution_environment()
        )

        env.set_parallelism(
            self.config.parallelism
        )

        env.enable_checkpointing(
            self.config.checkpoint_interval_ms
        )

        return env

    def _create_source(
        self,
        env: StreamExecutionEnvironment,
    ) -> DataStream:

        from pyflink.common import WatermarkStrategy
        from pyflink.datastream.connectors.file_system import (
            FileSource,
            StreamFormat,
        )

        source = (
            FileSource
            .for_record_stream_format(
                StreamFormat.text_line_format(),
                "/app/data/raw/flink-events.jsonl",
            )
            .monitor_continuously(
                Duration.of_seconds(1)
            )
            .build()
        )

        return (
            env.from_source(
                source,
                WatermarkStrategy.no_watermarks(),
                "flume-flink-file-source",
            )
        )

    def build_pipeline(self) -> Any:

        self.config.validate()

        env = self._create_environment()

        raw_events = self._create_source(env)

        parsed_events = (
            raw_events
            .map(
                lambda value: json.loads(value),
                output_type=(
                    Types.PICKLED_BYTE_ARRAY()
                ),
            )
            .name(
                "parse-json-events"
            )
        )

        valid_events = (
            parsed_events
            .filter(
                lambda event: (
                    bool(
                        event.get(
                            "event_id"
                        )
                    )
                    and bool(
                        event.get(
                            "event_type"
                        )
                    )
                    and bool(
                        event.get(
                            "event_timestamp"
                        )
                    )
                    and bool(
                        event.get(
                            "customer_id"
                        )
                    )
                )
            )
            .name(
                "filter-valid-events"
            )
        )

        watermark_strategy = (
            FlinkWatermarkStrategy
            .for_bounded_out_of_orderness(
                Duration.of_seconds(
                    self.config
                    .watermark_out_of_orderness_seconds
                )
            )
            .with_timestamp_assigner(
                EventTimestampAssigner()
            )
        )

        timestamped_events = (
            valid_events
            .assign_timestamps_and_watermarks(
                watermark_strategy
            )
            .name(
                "event-time-watermarks"
            )
        )

        keyed_events = (
            timestamped_events
            .key_by(
                lambda event: event.get(
                    "customer_id",
                    "unknown",
                )
            )
        )

        windowed_events = (
            keyed_events
            .window(
                SlidingEventTimeWindows.of(
                    Time.seconds(
                        self.config
                        .sliding_window_size_seconds
                    ),
                    Time.seconds(
                        self.config
                        .sliding_window_slide_seconds
                    ),
                )
            )
        )

        aggregated_events = (
            windowed_events
            .process(
                WindowAggregationFunction(),
                output_type=Types.STRING(),
            )
            .name(
                "sliding-window-aggregation"
            )
        )

        aggregated_events.print(
            "STREAMING-WINDOW"
        ).name(
            "streaming-window-console-sink"
        )

        self._pipeline = env

        logger.info(
            "Pipeline Flink construída: %s",
            self.config.job_name,
        )

        return env

    def start(self) -> None:

        if self._running:
            logger.warning(
                "O job Flink '%s' já está em execução.",
                self.config.job_name,
            )
            return

        if self._pipeline is None:
            self.build_pipeline()

        logger.info(
            "Iniciando job Flink '%s' com paralelismo %d.",
            self.config.job_name,
            self.config.parallelism,
        )

        self._running = True

        try:
            self._pipeline.execute(
                self.config.job_name
            )
        finally:
            self._running = False

    def stop(self) -> None:

        if not self._running:
            return

        logger.info(
            "Encerrando job Flink '%s'.",
            self.config.job_name,
        )

        self._running = False

    def healthcheck(self) -> bool:

        try:
            self.config.validate()
        except ValueError:
            return False

        return True


def create_flink_job(
    config: Optional[FlinkJobConfig] = None,
) -> FlinkStreamingJob:

    return FlinkStreamingJob(
        config=config
    )


def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "[%(name)s] %(message)s"
        ),
    )

    job = create_flink_job()

    if not job.healthcheck():
        raise RuntimeError(
            "Configuração inválida para o job Flink."
        )

    job.build_pipeline()

    job.start()


if __name__ == "__main__":
    main()