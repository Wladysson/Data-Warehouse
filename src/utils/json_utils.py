from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Iterator


class JsonUtils:

    @staticmethod
    def dumps(
        value: Any,
        *,
        ensure_ascii: bool = False,
        sort_keys: bool = False,
    ) -> str:
        """Serializa um objeto para JSON."""

        return json.dumps(
            value,
            ensure_ascii=ensure_ascii,
            sort_keys=sort_keys,
            default=JsonUtils._default_serializer,
        )

    @staticmethod
    def loads(
        value: str,
    ) -> Any:

        if not isinstance(value, str):
            raise TypeError(
                "value deve ser uma string."
            )

        return json.loads(value)

    @staticmethod
    def _default_serializer(
        value: Any,
    ) -> Any:

        from datetime import date, datetime
        from enum import Enum

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        if isinstance(value, Enum):
            return value.value

        if hasattr(value, "to_dict"):
            return value.to_dict()

        raise TypeError(
            f"Tipo não serializável: {type(value).__name__}"
        )

    @staticmethod
    def dump_file(
        value: Any,
        path: str | Path,
        *,
        ensure_ascii: bool = False,
        indent: int | None = 2,
    ) -> Path:

        output_path = Path(path)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with output_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                value,
                file,
                ensure_ascii=ensure_ascii,
                indent=indent,
                default=JsonUtils._default_serializer,
            )

        return output_path

    @staticmethod
    def load_file(
        path: str | Path,
    ) -> Any:

        input_path = Path(path)

        if not input_path.exists():
            raise FileNotFoundError(
                f"Arquivo JSON não encontrado: {input_path}"
            )

        with input_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            return json.load(file)

    @staticmethod
    def write_jsonl(
        records: Iterable[Any],
        path: str | Path,
    ) -> Path:

        output_path = Path(path)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with output_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            for record in records:
                file.write(
                    JsonUtils.dumps(record)
                )
                file.write("\n")

        return output_path

    @staticmethod
    def append_jsonl(
        record: Any,
        path: str | Path,
    ) -> Path:

        output_path = Path(path)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with output_path.open(
            "a",
            encoding="utf-8",
        ) as file:
            file.write(
                JsonUtils.dumps(record)
            )
            file.write("\n")

        return output_path

    @staticmethod
    def read_jsonl(
        path: str | Path,
    ) -> Iterator[dict[str, Any]]:

        input_path = Path(path)

        if not input_path.exists():
            raise FileNotFoundError(
                f"Arquivo JSONL não encontrado: {input_path}"
            )

        with input_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            for line_number, line in enumerate(
                file,
                start=1,
            ):
                normalized = line.strip()

                if not normalized:
                    continue

                try:
                    record = json.loads(
                        normalized
                    )
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"JSON inválido na linha {line_number}: "
                        f"{exc}"
                    ) from exc

                if not isinstance(record, dict):
                    raise ValueError(
                        f"O registro da linha {line_number} "
                        "deve ser um objeto JSON."
                    )

                yield record

    @staticmethod
    def count_jsonl(
        path: str | Path,
    ) -> int:

        return sum(
            1
            for _ in JsonUtils.read_jsonl(path)
        )

    @staticmethod
    def validate(
        value: str,
    ) -> bool:

        if not isinstance(value, str):
            return False

        try:
            json.loads(value)
        except json.JSONDecodeError:
            return False

        return True

    @staticmethod
    def pretty(
        value: Any,
    ) -> str:

        return json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            default=JsonUtils._default_serializer,
        )