from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Mapping

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HBaseClientConfig:

    host: str = "localhost"
    port: int = 9090
    timeout: int = 5000
    autoconnect: bool = True

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise ValueError("O host do HBase não pode ser vazio.")

        if not 1 <= self.port <= 65535:
            raise ValueError("A porta do HBase deve estar entre 1 e 65535.")

        if self.timeout <= 0:
            raise ValueError("O timeout deve ser maior que zero.")


class HBaseClient:
    

    def __init__(
        self,
        config: HBaseClientConfig | None = None,
    ) -> None:
        self.config = config or HBaseClientConfig()
        self._connection = None

        if self.config.autoconnect:
            self.connect()

    @property
    def connection(self):
        """Retorna a conexão ativa com o HBase."""

        if self._connection is None:
            raise RuntimeError("O cliente HBase não está conectado.")

        return self._connection

    def connect(self) -> None:
        """Estabelece conexão com o HBase."""

        try:
            import happybase
        except ImportError as exc:
            raise RuntimeError(
                "A biblioteca happybase não está instalada."
            ) from exc

        try:
            self._connection = happybase.Connection(
                host=self.config.host,
                port=self.config.port,
                timeout=self.config.timeout,
                autoconnect=False,
            )
            self._connection.open()
        except Exception as exc:
            self._connection = None

            raise RuntimeError(
                f"Não foi possível conectar ao HBase "
                f"{self.config.host}:{self.config.port}."
            ) from exc

        logger.info(
            "Conexão HBase estabelecida em %s:%s",
            self.config.host,
            self.config.port,
        )

    def close(self) -> None:

        if self._connection is not None:
            self._connection.close()
            self._connection = None

            logger.info("Conexão HBase encerrada.")

    def create_table(
        self,
        name: str,
        families: Mapping[str, Mapping[str, object]] | Iterable[str],
    ) -> None:

        table_name = self._validate_table_name(name)

        if isinstance(families, Mapping):
            normalized_families = {
                self._normalize_family(family): dict(options)
                for family, options in families.items()
            }
        else:
            normalized_families = {
                self._normalize_family(family): {}
                for family in families
            }

        if not normalized_families:
            raise ValueError(
                "A tabela HBase deve possuir pelo menos uma família."
            )

        if self.table_exists(table_name):
            logger.info("Tabela HBase já existe: %s", table_name)
            return

        self.connection.create_table(
            table_name,
            normalized_families,
        )

        logger.info("Tabela HBase criada: %s", table_name)

    def delete_table(
        self,
        name: str,
        *,
        disable: bool = True,
    ) -> None:

        table_name = self._validate_table_name(name)

        if not self.table_exists(table_name):
            return

        if disable:
            try:
                self.connection.disable_table(table_name)
            except Exception:
                logger.debug(
                    "Tabela %s já estava desabilitada.",
                    table_name,
                )

        self.connection.delete_table(
            table_name,
            disable=False,
        )

        logger.info("Tabela HBase removida: %s", table_name)

    def table_exists(self, name: str) -> bool:
        """Verifica se uma tabela existe."""

        table_name = self._validate_table_name(name)

        return bool(
            self.connection.table_exists(table_name),
        )

    def put(
        self,
        table_name: str,
        row_key: str | bytes,
        data: Mapping[str | bytes, str | bytes],
        *,
        timestamp: int | None = None,
    ) -> None:

        table = self.table(table_name)
        key = self._encode(row_key)

        normalized_data = {
            self._encode(column): self._encode(value)
            for column, value in data.items()
        }

        kwargs = {}

        if timestamp is not None:
            kwargs["timestamp"] = timestamp

        table.put(
            key,
            normalized_data,
            **kwargs,
        )

    def put_many(
        self,
        table_name: str,
        rows: Iterable[
            tuple[str | bytes, Mapping[str | bytes, str | bytes]]
        ],
        *,
        batch_size: int = 100,
    ) -> int:

        if batch_size <= 0:
            raise ValueError("batch_size deve ser maior que zero.")

        table = self.table(table_name)
        count = 0

        with table.batch(batch_size=batch_size) as batch:
            for row_key, data in rows:
                normalized_data = {
                    self._encode(column): self._encode(value)
                    for column, value in data.items()
                }

                batch.put(
                    self._encode(row_key),
                    normalized_data,
                )

                count += 1

        return count

    def get(
        self,
        table_name: str,
        row_key: str | bytes,
        *,
        columns: Iterable[str | bytes] | None = None,
    ) -> dict[bytes, bytes]:

        table = self.table(table_name)

        normalized_columns = None

        if columns is not None:
            normalized_columns = [
                self._encode(column)
                for column in columns
            ]

        return table.row(
            self._encode(row_key),
            columns=normalized_columns,
        )

    def scan(
        self,
        table_name: str,
        *,
        row_start: str | bytes | None = None,
        row_stop: str | bytes | None = None,
        columns: Iterable[str | bytes] | None = None,
        limit: int | None = None,
    ):

        if limit is not None and limit <= 0:
            raise ValueError("O limite deve ser maior que zero.")

        table = self.table(table_name)

        kwargs = {}

        if row_start is not None:
            kwargs["row_start"] = self._encode(row_start)

        if row_stop is not None:
            kwargs["row_stop"] = self._encode(row_stop)

        if columns is not None:
            kwargs["columns"] = [
                self._encode(column)
                for column in columns
            ]

        if limit is not None:
            kwargs["limit"] = limit

        return table.scan(**kwargs)

    def delete(
        self,
        table_name: str,
        row_key: str | bytes,
        *,
        columns: Iterable[str | bytes] | None = None,
    ) -> None:

        table = self.table(table_name)
        key = self._encode(row_key)

        if columns is None:
            table.delete(key)
            return

        normalized_columns = [
            self._encode(column)
            for column in columns
        ]

        table.delete(
            key,
            columns=normalized_columns,
        )

    def table(self, name: str):

        table_name = self._validate_table_name(name)

        if not self.table_exists(table_name):
            raise KeyError(
                f"Tabela HBase não encontrada: {table_name}"
            )

        return self.connection.table(table_name)

    def list_tables(self) -> list[str]:
        """Lista as tabelas existentes no HBase."""

        return [
            self._decode(name)
            for name in self.connection.tables()
        ]

    def healthcheck(self) -> bool:
        """Verifica a disponibilidade da conexão HBase."""

        try:
            self.connection.tables()
            return True
        except Exception:
            return False

    def describe(self) -> dict[str, object]:

        return {
            "host": self.config.host,
            "port": self.config.port,
            "timeout": self.config.timeout,
            "connected": self._connection is not None,
        }

    @staticmethod
    def _validate_table_name(name: str) -> str:
        if not name or not name.strip():
            raise ValueError("O nome da tabela não pode ser vazio.")

        return name.strip()

    @staticmethod
    def _normalize_family(family: str) -> str:
        normalized = family.strip().rstrip(":")

        if not normalized:
            raise ValueError(
                "O nome da família de colunas não pode ser vazio."
            )

        return normalized

    @staticmethod
    def _encode(value: str | bytes) -> bytes:
        if isinstance(value, bytes):
            return value

        return str(value).encode("utf-8")

    @staticmethod
    def _decode(value: bytes) -> str:
        return value.decode("utf-8")