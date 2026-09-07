from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Optional


logger = logging.getLogger(__name__)


class FlumeAgent:

    def __init__(
        self,
        config_path: str | Path = (
            "configs/flume/flume-conf.properties"
        ),
        flume_home: Optional[str | Path] = None,
        agent_name: str = "agent",
    ) -> None:
        self.config_path = Path(config_path)
        self.flume_home = (
            Path(flume_home)
            if flume_home is not None
            else None
        )
        self.agent_name = agent_name

    @property
    def executable(self) -> str:
        """
        Retorna o caminho do executável do Flume.
        """

        if self.flume_home is None:
            return "flume-ng"

        return str(
            self.flume_home / "bin" / "flume-ng"
        )

    def configuration_exists(self) -> bool:
        """
        Verifica se a configuração do agente existe.
        """

        return self.config_path.is_file()

    def validate_configuration(self) -> None:
        """
        Valida a existência da configuração necessária para
        iniciar o agente.
        """

        if not self.configuration_exists():
            raise FileNotFoundError(
                f"Configuração do Flume não encontrada: "
                f"{self.config_path}"
            )

    def build_command(self) -> list[str]:
        """
        Monta o comando de execução do agente Flume.
        """

        self.validate_configuration()

        return [
            self.executable,
            "agent",
            "--name",
            self.agent_name,
            "--conf-file",
            str(self.config_path),
        ]

    def start(
        self,
        wait: bool = False,
    ) -> subprocess.Popen[bytes] | subprocess.CompletedProcess[bytes]:
        """
        Inicia o agente Flume.

        Por padrão, o processo é iniciado em background.
        """

        command = self.build_command()

        logger.info(
            "Iniciando agente Flume '%s'.",
            self.agent_name,
        )

        if wait:
            return subprocess.run(
                command,
                check=True,
            )

        return subprocess.Popen(
            command,
        )

    def check_configuration(self) -> dict[str, object]:
        """
        Retorna informações básicas sobre a configuração.
        """

        exists = self.configuration_exists()

        return {
            "agent_name": self.agent_name,
            "config_path": str(self.config_path),
            "config_exists": exists,
            "executable": self.executable,
        }

    def healthcheck(self) -> bool:
        """
        Verifica se o arquivo de configuração do agente está
        disponível e pronto para execução.
        """

        try:
            self.validate_configuration()
        except FileNotFoundError:
            return False

        return True