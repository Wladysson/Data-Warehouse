from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import Any, Dict, Iterator, Optional

from src.streaming.models import StreamingEvent
from src.streaming.sources.file_source import FileSource


logger = logging.getLogger(__name__)


class HdfsSource(FileSource):

    def __init__(
        self,
        input_path: str = (
            "hdfs://namenode:9000/data/raw/events"
        ),
        validator=None,
        hdfs_host: str = "namenode",
        hdfs_port: int = 9000,
    ) -> None:
        super().__init__(
            input_path=input_path,
            validator=validator,
        )

        if not hdfs_host.strip():
            raise ValueError(
                "hdfs_host não pode ser vazio."
            )

        if hdfs_port <= 0:
            raise ValueError(
                "hdfs_port deve ser maior que zero."
            )

        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port

    @property
    def hdfs_uri(self) -> str:
        return (
            f"hdfs://{self.hdfs_host}:{self.hdfs_port}"
        )

    def normalized_hdfs_path(self) -> str:
        path = self.input_path

        if path.startswith("hdfs://"):
            return path

        normalized_path = str(
            PurePosixPath(path)
        )

        if not normalized_path.startswith("/"):
            normalized_path = (
                f"/{normalized_path}"
            )

        return (
            f"{self.hdfs_uri}"
            f"{normalized_path}"
        )

    def exists(self) -> bool:
        
        try:
            from pyarrow import fs

            filesystem, path = (
                fs.FileSystem.from_uri(
                    self.normalized_hdfs_path()
                )
            )

            info = filesystem.get_file_info(path)

            return (
                info.type
                != fs.FileType.NotFound
            )

        except ImportError:
            logger.warning(
                "pyarrow não está instalado; "
                "não foi possível consultar o HDFS."
            )
            return False

        except Exception as exc:
            logger.warning(
                "Falha ao consultar o HDFS: %s",
                exc,
            )
            return False

    def read_lines(self) -> Iterator[str]:
        """
        Lê registros JSON Lines diretamente do HDFS.
        """

        try:
            from pyarrow import fs

            filesystem, path = (
                fs.FileSystem.from_uri(
                    self.normalized_hdfs_path()
                )
            )

            info = filesystem.get_file_info(path)

            if info.type == fs.FileType.NotFound:
                raise FileNotFoundError(
                    "Origem HDFS não encontrada: "
                    f"{self.normalized_hdfs_path()}"
                )

            if info.type != fs.FileType.File:
                raise ValueError(
                    "A origem HDFS deve apontar para um arquivo."
                )

            with filesystem.open_input_file(path) as input_file:
                for raw_line in input_file:
                    if isinstance(raw_line, bytes):
                        line = raw_line.decode(
                            "utf-8"
                        )
                    else:
                        line = str(raw_line)

                    normalized_line = line.strip()

                    if normalized_line:
                        yield normalized_line

        except ImportError as exc:
            raise RuntimeError(
                "A leitura HDFS requer o pacote pyarrow."
            ) from exc

    def stream(self) -> Iterator[StreamingEvent]:

        for event in self.read_validated_events():
            try:
                yield self.to_streaming_event(event)

            except (TypeError, ValueError) as exc:
                logger.warning(
                    "Evento HDFS rejeitado durante "
                    "a conversão: %s",
                    exc,
                )

    def describe(self) -> Dict[str, Any]:
        return {
            "type": "hdfs",
            "input_path": self.input_path,
            "normalized_path": (
                self.normalized_hdfs_path()
            ),
            "hdfs_uri": self.hdfs_uri,
            "hdfs_host": self.hdfs_host,
            "hdfs_port": self.hdfs_port,
            "exists": self.exists(),
        }

    def healthcheck(self) -> bool:
        return self.exists()