from __future__ import annotations

import json
import logging
import posixpath
import subprocess
from dataclasses import dataclass
from typing import Iterable

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HDFSClientConfig:
    """Configurações de conexão e execução do cliente HDFS."""

    hdfs_uri: str = "hdfs://localhost:9000"
    hdfs_bin: str = "hdfs"
    user: str | None = None
    command_timeout: int = 60

    def __post_init__(self) -> None:
        if not self.hdfs_uri.strip():
            raise ValueError("A URI do HDFS não pode ser vazia.")

        if not self.hdfs_bin.strip():
            raise ValueError("O executável do HDFS não pode ser vazio.")

        if self.command_timeout <= 0:
            raise ValueError("O timeout dos comandos deve ser maior que zero.")


class HDFSClient:

    def __init__(self, config: HDFSClientConfig | None = None) -> None:
        self.config = config or HDFSClientConfig()

    def _normalize_path(self, path: str) -> str:
        if not path or not path.strip():
            raise ValueError("O caminho HDFS não pode ser vazio.")

        normalized = path.strip()

        if normalized.startswith("hdfs://"):
            return normalized

        if not normalized.startswith("/"):
            normalized = f"/{normalized}"

        return normalized

    def _build_target(self, path: str | None = None) -> str:
        if path is None:
            return self.config.hdfs_uri

        normalized_path = self._normalize_path(path)

        if normalized_path.startswith("hdfs://"):
            return normalized_path

        return f"{self.config.hdfs_uri.rstrip('/')}{normalized_path}"

    def _run(
        self,
        command: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        logger.debug("Executando comando HDFS: %s", " ".join(command))

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.config.command_timeout,
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"Executável do HDFS não encontrado: {self.config.hdfs_bin}"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(
                f"Comando HDFS excedeu o timeout de "
                f"{self.config.command_timeout}s."
            ) from exc

        if check and result.returncode != 0:
            stderr = result.stderr.strip()
            stdout = result.stdout.strip()

            details = stderr or stdout or "erro desconhecido"

            raise RuntimeError(
                f"Comando HDFS falhou com código {result.returncode}: {details}"
            )

        return result

    def _command(self, *args: str) -> list[str]:
        command = [self.config.hdfs_bin, "dfs"]

        if self.config.user:
            command.extend(["-D", f"hadoop.job.ugi={self.config.user}"])

        command.extend(args)

        return command

    def exists(self, path: str) -> bool:
        """Verifica se um caminho existe no HDFS."""

        target = self._build_target(path)

        result = self._run(
            self._command("-test", "-e", target),
            check=False,
        )

        return result.returncode == 0

    def mkdir(self, path: str, *, parents: bool = True) -> None:
        """Cria um diretório no HDFS."""

        target = self._build_target(path)

        args = ["-mkdir"]

        if parents:
            args.append("-p")

        args.append(target)

        self._run(self._command(*args))

        logger.info("Diretório HDFS criado: %s", target)

    def delete(
        self,
        path: str,
        *,
        recursive: bool = False,
        skip_trash: bool = False,
    ) -> None:

        target = self._build_target(path)

        args = ["-rm"]

        if recursive:
            args.append("-r")

        if skip_trash:
            args.append("-skipTrash")

        args.append(target)

        self._run(self._command(*args))

        logger.info("Caminho HDFS removido: %s", target)

    def list(self, path: str = "/") -> list[str]:

        target = self._build_target(path)

        result = self._run(self._command("-ls", target))

        entries: list[str] = []

        for line in result.stdout.splitlines():
            line = line.strip()

            if not line or line.startswith("Found "):
                continue

            parts = line.split()

            if parts:
                entries.append(parts[-1])

        return entries

    def mkdirs(self, paths: Iterable[str]) -> None:

        for path in paths:
            self.mkdir(path)

    def put(
        self,
        local_path: str,
        hdfs_path: str,
        *,
        overwrite: bool = False,
    ) -> None:

        target = self._build_target(hdfs_path)

        args = ["-put"]

        if overwrite:
            args.append("-f")

        args.extend([local_path, target])

        self._run(self._command(*args))

        logger.info(
            "Arquivo enviado para HDFS: %s -> %s",
            local_path,
            target,
        )

    def get(
        self,
        hdfs_path: str,
        local_path: str,
        *,
        overwrite: bool = False,
    ) -> None:

        source = self._build_target(hdfs_path)

        args = ["-get"]

        if overwrite:
            args.append("-f")

        args.extend([source, local_path])

        self._run(self._command(*args))

        logger.info(
            "Arquivo baixado do HDFS: %s -> %s",
            source,
            local_path,
        )

    def cat(self, path: str) -> str:

        target = self._build_target(path)

        result = self._run(self._command("-cat", target))

        return result.stdout

    def write_text(
        self,
        path: str,
        content: str,
        *,
        overwrite: bool = True,
    ) -> None:

        temp_path = f"/tmp/hdfs-client-{id(content)}.tmp"

        try:
            import tempfile

            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                delete=False,
                suffix=".tmp",
            ) as temporary_file:
                temporary_file.write(content)
                local_path = temporary_file.name

            self.put(
                local_path,
                path,
                overwrite=overwrite,
            )
        finally:
            try:
                import os

                if "local_path" in locals():
                    os.unlink(local_path)
            except OSError:
                logger.warning(
                    "Não foi possível remover o arquivo temporário local."
                )

    def write_json(
        self,
        path: str,
        data: object,
        *,
        overwrite: bool = True,
    ) -> None:

        content = json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

        self.write_text(
            path,
            content,
            overwrite=overwrite,
        )

    def du(self, path: str) -> int:

        target = self._build_target(path)

        result = self._run(
            self._command("-du", "-s", target),
        )

        lines = [
            line.strip()
            for line in result.stdout.splitlines()
            if line.strip()
        ]

        if not lines:
            return 0

        parts = lines[-1].split()

        if not parts:
            return 0

        return int(parts[0])

    def count(self, path: str) -> dict[str, int]:

        target = self._build_target(path)

        result = self._run(
            self._command("-count", target),
        )

        lines = [
            line.strip()
            for line in result.stdout.splitlines()
            if line.strip()
        ]

        if not lines:
            return {
                "directories": 0,
                "files": 0,
                "bytes": 0,
            }

        parts = lines[-1].split()

        if len(parts) < 3:
            raise RuntimeError(
                f"Resposta inesperada do comando hdfs dfs -count: "
                f"{result.stdout}"
            )

        return {
            "directories": int(parts[0]),
            "files": int(parts[1]),
            "bytes": int(parts[2]),
        }

    def healthcheck(self) -> bool:
        """Verifica se o cliente consegue consultar o HDFS."""

        try:
            result = self._run(
                self._command("-ls", self.config.hdfs_uri),
                check=False,
            )
        except (RuntimeError, TimeoutError):
            return False

        return result.returncode == 0

    def get_capacity(self) -> dict[str, int]:
        """Obtém capacidade e utilização do filesystem HDFS."""

        result = self._run(
            self._command("-df", "-h", self.config.hdfs_uri),
        )

        lines = [
            line.strip()
            for line in result.stdout.splitlines()
            if line.strip()
        ]

        if len(lines) < 2:
            raise RuntimeError(
                "Não foi possível interpretar a capacidade do HDFS."
            )

        parts = lines[-1].split()

        if len(parts) < 4:
            raise RuntimeError(
                f"Resposta inesperada do comando hdfs dfs -df: "
                f"{result.stdout}"
            )

        return {
            "capacity": self._parse_size(parts[1]),
            "used": self._parse_size(parts[2]),
            "available": self._parse_size(parts[3]),
        }

    @staticmethod
    def _parse_size(value: str) -> int:
        """Converte valores de tamanho do HDFS para bytes."""

        normalized = value.strip().upper()

        units = {
            "B": 1,
            "K": 1024,
            "M": 1024**2,
            "G": 1024**3,
            "T": 1024**4,
            "P": 1024**5,
        }

        if normalized[-1:] in units:
            number = float(normalized[:-1])
            return int(number * units[normalized[-1]])

        return int(float(normalized))

    def join_path(self, *parts: str) -> str:

        if not parts:
            raise ValueError("É necessário informar ao menos um componente.")

        cleaned_parts = [
            part.strip("/")
            for part in parts
            if part and part.strip()
        ]

        if not cleaned_parts:
            return "/"

        return posixpath.join("/", *cleaned_parts)

    def describe(self) -> dict[str, object]:
        """Retorna a configuração efetiva do cliente."""

        return {
            "hdfs_uri": self.config.hdfs_uri,
            "hdfs_bin": self.config.hdfs_bin,
            "user": self.config.user,
            "command_timeout": self.config.command_timeout,
        }