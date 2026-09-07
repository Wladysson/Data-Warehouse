from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, Optional

from src.streaming.models import (
    AlertEvent,
    StreamingEvent,
    WindowAggregation,
)


logger = logging.getLogger(__name__)


class HdfsSink:

    def __init__(
        self,
        base_path: str = (
            "hdfs://namenode:9000/data/raw/streaming"
        ),
        hdfs_host: str = "namenode",
        hdfs_port: int = 9000,
    ) -> None:
        if not base_path.strip():
            raise ValueError(
                "base_path não pode ser vazio."
            )

        if not hdfs_host.strip():
            raise ValueError(
                "hdfs_host não pode ser vazio."
            )

        if hdfs_port <= 0:
            raise ValueError(
                "hdfs_port deve ser maior que zero."
            )

        self.base_path = base_path.rstrip("/")
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port

        self._filesystem: Any = None

    @property
    def hdfs_uri(self) -> str:
        return (
            f"hdfs://"
            f"{self.hdfs_host}:"
            f"{self.hdfs_port}"
        )

    @property
    def connected(self) -> bool:
        return self._filesystem is not None

    def normalized_base_path(self) -> str:
        if self.base_path.startswith(
            "hdfs://"
        ):
            return self.base_path

        normalized_path = str(
            PurePosixPath(
                self.base_path
            )
        )

        if not normalized_path.startswith("/"):
            normalized_path = (
                f"/{normalized_path}"
            )

        return (
            f"{self.hdfs_uri}"
            f"{normalized_path}"
        )

    def connect(self) -> None:

        if self.connected:
            return

        try:
            from pyarrow import fs

        except ImportError as exc:
            raise RuntimeError(
                "O sink HDFS requer o pacote pyarrow."
            ) from exc

        filesystem, _ = fs.FileSystem.from_uri(
            self.normalized_base_path()
        )

        self._filesystem = filesystem

        logger.info(
            "Filesystem HDFS conectado: %s",
            self.normalized_base_path(),
        )

    def close(self) -> None:
        self._filesystem = None

        logger.info(
            "Cliente HDFS encerrado."
        )

    def build_partition_path(
        self,
        timestamp: datetime,
    ) -> str:
        
        if timestamp.tzinfo is None:
            raise ValueError(
                "timestamp deve possuir timezone."
            )

        return (
            f"{self.normalized_base_path()}"
            f"/ano={timestamp.year:04d}"
            f"/mes={timestamp.month:02d}"
            f"/dia={timestamp.day:02d}"
            f"/hora={timestamp.hour:02d}"
        )

    def build_file_path(
        self,
        timestamp: datetime,
        prefix: str = "streaming",
    ) -> str:

        if not prefix.strip():
            raise ValueError(
                "prefix não pode ser vazio."
            )

        partition_path = (
            self.build_partition_path(
                timestamp
            )
        )

        return (
            f"{partition_path}/"
            f"{prefix}_"
            f"{timestamp.strftime('%Y%m%d%H%M%S')}"
            f".jsonl"
        )

    def serialize(
        self,
        value: Any,
    ) -> str:

        if isinstance(
            value,
            StreamingEvent,
        ):
            payload = value.to_dict()

        elif isinstance(
            value,
            AlertEvent,
        ):
            payload = value.to_dict()

        elif isinstance(
            value,
            WindowAggregation,
        ):
            payload = value.to_dict()

        elif isinstance(
            value,
            dict,
        ):
            payload = value

        else:
            raise TypeError(
                "Tipo não suportado pelo HdfsSink: "
                f"{type(value).__name__}"
            )

        return json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def write(
        self,
        value: Any,
        timestamp: Optional[
            datetime
        ] = None,
        prefix: str = "streaming",
    ) -> str:

        self.connect()

        timestamp = (
            timestamp
            or datetime.now(timezone.utc)
        )

        file_path = self.build_file_path(
            timestamp,
            prefix=prefix,
        )

        partition_path = (
            self.build_partition_path(
                timestamp
            )
        )

        partition_uri = partition_path

        if partition_uri.startswith(
            self.normalized_base_path()
        ):
            relative_partition = (
                partition_uri[
                    len(
                        self.normalized_base_path()
                    ):
                ].lstrip("/")
            )
        else:
            relative_partition = ""

        if relative_partition:
            self._filesystem.create_dir(
                relative_partition,
                recursive=True,
            )

        payload = (
            self.serialize(value)
            + "\n"
        ).encode("utf-8")

        relative_file = file_path[
            len(
                self.normalized_base_path()
            ):
        ].lstrip("/")

        with self._filesystem.open_output_stream(
            relative_file
        ) as output:
            output.write(payload)

        logger.info(
            "Registro persistido no HDFS: %s",
            file_path,
        )

        return file_path

    def write_many(
        self,
        values: Iterable[Any],
        timestamp: Optional[
            datetime
        ] = None,
        prefix: str = "streaming",
    ) -> int:

        values_list = list(values)

        if not values_list:
            return 0

        self.connect()

        timestamp = (
            timestamp
            or datetime.now(timezone.utc)
        )

        file_path = self.build_file_path(
            timestamp,
            prefix=prefix,
        )

        partition_path = (
            self.build_partition_path(
                timestamp
            )
        )

        relative_partition = partition_path[
            len(
                self.normalized_base_path()
            ):
        ].lstrip("/")

        if relative_partition:
            self._filesystem.create_dir(
                relative_partition,
                recursive=True,
            )

        content = "".join(
            self.serialize(value)
            + "\n"
            for value in values_list
        ).encode("utf-8")

        relative_file = file_path[
            len(
                self.normalized_base_path()
            ):
        ].lstrip("/")

        with self._filesystem.open_output_stream(
            relative_file
        ) as output:
            output.write(content)

        logger.info(
            "%d registros persistidos no HDFS: %s",
            len(values_list),
            file_path,
        )

        return len(values_list)

    def write_event(
        self,
        event: StreamingEvent,
    ) -> str:

        if not isinstance(
            event,
            StreamingEvent,
        ):
            raise TypeError(
                "event deve ser uma instância de StreamingEvent."
            )

        return self.write(
            event,
            timestamp=event.event_timestamp,
            prefix="events",
        )

    def write_alert(
        self,
        alert: AlertEvent,
    ) -> str:

        if not isinstance(
            alert,
            AlertEvent,
        ):
            raise TypeError(
                "alert deve ser uma instância de AlertEvent."
            )

        return self.write(
            alert,
            timestamp=alert.event_timestamp,
            prefix="alerts",
        )

    def write_aggregation(
        self,
        aggregation: WindowAggregation,
    ) -> str:
        """
        Persiste uma agregação de janela.
        """

        if not isinstance(
            aggregation,
            WindowAggregation,
        ):
            raise TypeError(
                "aggregation deve ser uma instância "
                "de WindowAggregation."
            )

        return self.write(
            aggregation,
            timestamp=aggregation.window_start,
            prefix="aggregations",
        )

    def healthcheck(self) -> bool:

        try:
            self.connect()

            self._filesystem.get_file_info(
                self.normalized_base_path()
                .replace(
                    self.hdfs_uri,
                    "",
                )
                .lstrip("/")
            )

            return True

        except Exception as exc:
            logger.warning(
                "Healthcheck HDFS falhou: %s",
                exc,
            )
            return False

    def describe(self) -> Dict[str, Any]:
        return {
            "type": "hdfs",
            "base_path": self.base_path,
            "normalized_base_path": (
                self.normalized_base_path()
            ),
            "hdfs_uri": self.hdfs_uri,
            "hdfs_host": self.hdfs_host,
            "hdfs_port": self.hdfs_port,
            "connected": self.connected,
        }