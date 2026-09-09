from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HiveClientConfig:

    host: str = "localhost"
    port: int = 10000
    username: str = "hive"
    database: str = "ecommerce"
    auth_mechanism: str = "NOSASL"
    timeout: int = 30

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise ValueError("O host do Hive não pode ser vazio.")

        if not 1 <= self.port <= 65535:
            raise ValueError("A porta do Hive deve estar entre 1 e 65535.")

        if not self.username.strip():
            raise ValueError("O usuário do Hive não pode ser vazio.")

        if not self.database.strip():
            raise ValueError("O banco padrão do Hive não pode ser vazio.")

        if self.timeout <= 0:
            raise ValueError("O timeout deve ser maior que zero.")


class HiveClient:

    def __init__(
        self,
        config: HiveClientConfig | None = None,
    ) -> None:
        self.config = config or HiveClientConfig()
        self._connection = None

    @property
    def connection(self):

        if self._connection is None:
            raise RuntimeError("O cliente Hive não está conectado.")

        return self._connection

    def connect(self) -> None:

        try:
            from pyhive import hive
        except ImportError as exc:
            raise RuntimeError(
                "A biblioteca PyHive não está instalada."
            ) from exc

        try:
            self._connection = hive.Connection(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                database=self.config.database,
                auth=self.config.auth_mechanism,
            )
        except Exception as exc:
            self._connection = None

            raise RuntimeError(
                f"Não foi possível conectar ao HiveServer2 "
                f"{self.config.host}:{self.config.port}."
            ) from exc

        logger.info(
            "Conexão Hive estabelecida em %s:%s",
            self.config.host,
            self.config.port,
        )

    def close(self) -> None:
        """Fecha a conexão com o Hive."""

        if self._connection is not None:
            self._connection.close()
            self._connection = None

            logger.info("Conexão Hive encerrada.")

    def reconnect(self) -> None:

        self.close()
        self.connect()

    def execute(
        self,
        sql: str,
        *,
        fetch: bool = False,
    ) -> list[tuple[Any, ...]] | None:

        if not sql or not sql.strip():
            raise ValueError("A instrução SQL não pode ser vazia.")

        cursor = self.connection.cursor()

        try:
            cursor.execute(sql)

            if fetch:
                return cursor.fetchall()

            return None
        finally:
            cursor.close()

    def execute_many(
        self,
        statements: list[str],
    ) -> None:

        if not statements:
            return

        for statement in statements:
            self.execute(statement)

    def query(
        self,
        sql: str,
    ) -> list[tuple[Any, ...]]:

        result = self.execute(
            sql,
            fetch=True,
        )

        return result or []

    def create_database(
        self,
        database: str,
        *,
        location: str | None = None,
        if_not_exists: bool = True,
    ) -> None:

        normalized = self._validate_identifier(database)

        statement = "CREATE DATABASE"

        if if_not_exists:
            statement += " IF NOT EXISTS"

        statement += f" `{normalized}`"

        if location:
            statement += f" LOCATION '{self._escape_sql(location)}'"

        self.execute(statement)

        logger.info(
            "Banco Hive criado ou validado: %s",
            normalized,
        )

    def drop_database(
        self,
        database: str,
        *,
        cascade: bool = False,
    ) -> None:

        normalized = self._validate_identifier(database)

        statement = f"DROP DATABASE IF EXISTS `{normalized}`"

        if cascade:
            statement += " CASCADE"

        self.execute(statement)

    def use_database(self, database: str) -> None:

        normalized = self._validate_identifier(database)

        self.execute(
            f"USE `{normalized}`",
        )

    def database_exists(self, database: str) -> bool:

        normalized = self._validate_identifier(database)

        result = self.query(
            f"SHOW DATABASES LIKE '{self._escape_sql(normalized)}'",
        )

        return any(
            row and str(row[0]).lower() == normalized.lower()
            for row in result
        )

    def list_databases(self) -> list[str]:

        result = self.query("SHOW DATABASES")

        return [
            str(row[0])
            for row in result
            if row
        ]

    def table_exists(
        self,
        table: str,
        *,
        database: str | None = None,
    ) -> bool:

        normalized_table = self._validate_identifier(table)
        normalized_database = (
            self._validate_identifier(database)
            if database
            else None
        )

        qualified_name = (
            f"`{normalized_database}`.`{normalized_table}`"
            if normalized_database
            else f"`{normalized_table}`"
        )

        try:
            self.execute(
                f"DESCRIBE `{normalized_database}`.`{normalized_table}`"
                if normalized_database
                else f"DESCRIBE `{normalized_table}`",
            )

            return True
        except Exception:
            logger.debug(
                "Tabela Hive não encontrada: %s",
                qualified_name,
            )
            return False

    def list_tables(
        self,
        database: str | None = None,
    ) -> list[str]:

        if database:
            self.use_database(database)

        result = self.query("SHOW TABLES")

        tables: list[str] = []

        for row in result:
            if not row:
                continue

            tables.append(str(row[-1]))

        return tables

    def describe_table(
        self,
        table: str,
        *,
        database: str | None = None,
    ) -> list[tuple[Any, ...]]:

        normalized_table = self._validate_identifier(table)

        if database:
            normalized_database = self._validate_identifier(database)

            sql = (
                f"DESCRIBE `{normalized_database}`."
                f"`{normalized_table}`"
            )
        else:
            sql = f"DESCRIBE `{normalized_table}`"

        return self.query(sql)

    def show_partitions(
        self,
        table: str,
        *,
        database: str | None = None,
    ) -> list[tuple[Any, ...]]:

        normalized_table = self._validate_identifier(table)

        if database:
            normalized_database = self._validate_identifier(database)

            sql = (
                f"SHOW PARTITIONS `{normalized_database}`."
                f"`{normalized_table}`"
            )
        else:
            sql = f"SHOW PARTITIONS `{normalized_table}`"

        return self.query(sql)

    def healthcheck(self) -> bool:
        """Verifica a disponibilidade do HiveServer2."""

        try:
            self.query("SELECT 1")
            return True
        except Exception:
            return False

    def describe(self) -> dict[str, object]:

        return {
            "host": self.config.host,
            "port": self.config.port,
            "username": self.config.username,
            "database": self.config.database,
            "auth_mechanism": self.config.auth_mechanism,
            "timeout": self.config.timeout,
            "connected": self._connection is not None,
        }

    @staticmethod
    def _validate_identifier(value: str) -> str:
        if not value or not value.strip():
            raise ValueError("O identificador Hive não pode ser vazio.")

        normalized = value.strip()

        if any(
            character in normalized
            for character in (";", "'", '"', "`", "\n", "\r")
        ):
            raise ValueError(
                f"Identificador Hive inválido: {value}"
            )

        return normalized

    @staticmethod
    def _escape_sql(value: str) -> str:
        return value.replace("'", "''")