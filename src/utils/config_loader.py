from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


class ConfigLoader:

    def __init__(
        self,
        config_directory: str | Path = "configs/environments",
    ) -> None:
        self.config_directory = Path(config_directory)

    def _validate_environment(
        self,
        environment: str,
    ) -> str:

        if not isinstance(environment, str):
            raise TypeError(
                "environment deve ser uma string."
            )

        normalized = environment.strip().lower()

        if not normalized:
            raise ValueError(
                "environment não pode ser vazio."
            )

        return normalized

    def _environment_path(
        self,
        environment: str,
    ) -> Path:

        normalized = self._validate_environment(
            environment
        )

        path = self.config_directory / (
            f"{normalized}.yaml"
        )

        if not path.exists():
            raise FileNotFoundError(
                f"Arquivo de configuração não encontrado: {path}"
            )

        return path

    def load_file(
        self,
        path: str | Path,
    ) -> dict[str, Any]:

        config_path = Path(path)

        if not config_path.exists():
            raise FileNotFoundError(
                f"Arquivo de configuração não encontrado: {config_path}"
            )

        if not config_path.is_file():
            raise ValueError(
                f"O caminho informado não é um arquivo: {config_path}"
            )

        with config_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            data = yaml.safe_load(file)

        if data is None:
            return {}

        if not isinstance(data, dict):
            raise ValueError(
                "A configuração YAML deve possuir um objeto na raiz."
            )

        return data

    def load_environment(
        self,
        environment: str,
    ) -> dict[str, Any]:

        path = self._environment_path(environment)

        return self.load_file(path)

    def load(
        self,
        environment: str | None = None,
    ) -> dict[str, Any]:

        selected_environment = (
            environment
            or os.getenv(
                "PIPELINE_ENVIRONMENT",
                "dev",
            )
        )

        return self.load_environment(
            selected_environment
        )

    @staticmethod
    def get(
        config: dict[str, Any],
        path: str,
        default: Any = None,
    ) -> Any:

        if not isinstance(config, dict):
            raise TypeError(
                "config deve ser um dicionário."
            )

        if not path.strip():
            return default

        current: Any = config

        for key in path.split("."):
            if not isinstance(current, dict):
                return default

            if key not in current:
                return default

            current = current[key]

        return current

    @staticmethod
    def require(
        config: dict[str, Any],
        path: str,
    ) -> Any:

        sentinel = object()

        value = ConfigLoader.get(
            config,
            path,
            sentinel,
        )

        if value is sentinel:
            raise KeyError(
                f"Configuração obrigatória não encontrada: {path}"
            )

        return value

    @staticmethod
    def merge(
        base: dict[str, Any],
        override: dict[str, Any],
    ) -> dict[str, Any]:

        if not isinstance(base, dict):
            raise TypeError(
                "base deve ser um dicionário."
            )

        if not isinstance(override, dict):
            raise TypeError(
                "override deve ser um dicionário."
            )

        result = dict(base)

        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = ConfigLoader.merge(
                    result[key],
                    value,
                )
            else:
                result[key] = value

        return result

    @staticmethod
    def expand_environment(
        config: dict[str, Any],
    ) -> dict[str, Any]:

        def resolve(value: Any) -> Any:
            if isinstance(value, dict):
                return {
                    key: resolve(item)
                    for key, item in value.items()
                }

            if isinstance(value, list):
                return [
                    resolve(item)
                    for item in value
                ]

            if isinstance(value, str):
                result = value

                for key, env_value in os.environ.items():
                    result = result.replace(
                        f"${{{key}}}",
                        env_value,
                    )

                return result

            return value

        return resolve(config)

    def load_resolved(
        self,
        environment: str | None = None,
    ) -> dict[str, Any]:

        config = self.load(environment)

        return self.expand_environment(config)

    def available_environments(self) -> tuple[str, ...]:
        """Retorna os ambientes YAML disponíveis."""

        if not self.config_directory.exists():
            return ()

        environments = sorted(
            path.stem
            for path in self.config_directory.glob("*.yaml")
            if path.is_file()
        )

        return tuple(environments)

    def describe(self) -> dict[str, Any]:

        return {
            "config_directory": str(
                self.config_directory
            ),
            "directory_exists": (
                self.config_directory.exists()
            ),
            "available_environments": (
                self.available_environments()
            ),
        }