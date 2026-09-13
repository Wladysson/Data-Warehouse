from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Any


class LoggingUtils:

    DEFAULT_FORMAT = (
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    )

    DEFAULT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

    @staticmethod
    def get_logger(
        name: str,
        level: int = logging.INFO,
    ) -> logging.Logger:

        if not name.strip():
            raise ValueError(
                "name não pode ser vazio."
            )

        logger = logging.getLogger(name)

        if not logger.handlers:
            LoggingUtils.configure_logger(
                logger,
                level=level,
            )

        return logger

    @staticmethod
    def configure_logger(
        logger: logging.Logger,
        level: int = logging.INFO,
        stream: Any = None,
        formatter: logging.Formatter | None = None,
    ) -> logging.Logger:

        if not isinstance(logger, logging.Logger):
            raise TypeError(
                "logger deve ser uma instância de logging.Logger."
            )

        logger.setLevel(level)
        logger.propagate = False

        if not logger.handlers:
            handler = logging.StreamHandler(
                stream or sys.stdout
            )

            handler.setLevel(level)

            handler.setFormatter(
                formatter
                or LoggingUtils.default_formatter()
            )

            logger.addHandler(handler)

        return logger

    @staticmethod
    def default_formatter() -> logging.Formatter:

        return logging.Formatter(
            LoggingUtils.DEFAULT_FORMAT,
            datefmt=LoggingUtils.DEFAULT_DATE_FORMAT,
        )

    @staticmethod
    def add_file_handler(
        logger: logging.Logger,
        file_path: str | Path,
        level: int = logging.INFO,
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 5,
    ) -> logging.handlers.RotatingFileHandler:

        if max_bytes < 1:
            raise ValueError(
                "max_bytes deve ser maior que zero."
            )

        if backup_count < 0:
            raise ValueError(
                "backup_count não pode ser negativo."
            )

        path = Path(file_path)
        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        handler = logging.handlers.RotatingFileHandler(
            path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )

        handler.setLevel(level)
        handler.setFormatter(
            LoggingUtils.default_formatter()
        )

        logger.addHandler(handler)

        return handler

    @staticmethod
    def set_level(
        logger: logging.Logger,
        level: int,
    ) -> None:

        logger.setLevel(level)

        for handler in logger.handlers:
            handler.setLevel(level)

    @staticmethod
    def level_from_string(
        level: str,
    ) -> int:

        if not isinstance(level, str):
            raise TypeError(
                "level deve ser uma string."
            )

        normalized = level.strip().upper()

        levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "WARN": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
            "FATAL": logging.CRITICAL,
        }

        if normalized not in levels:
            raise ValueError(
                f"Nível de logging inválido: {level}."
            )

        return levels[normalized]

    @staticmethod
    def configure_root(
        level: int = logging.INFO,
    ) -> None:

        root_logger = logging.getLogger()

        if root_logger.handlers:
            LoggingUtils.set_level(
                root_logger,
                level,
            )
            return

        logging.basicConfig(
            level=level,
            format=LoggingUtils.DEFAULT_FORMAT,
            datefmt=LoggingUtils.DEFAULT_DATE_FORMAT,
        )

    @staticmethod
    def shutdown() -> None:

        logging.shutdown()