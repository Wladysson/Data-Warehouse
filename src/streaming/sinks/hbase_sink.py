from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from src.streaming.models import (
    AlertEvent,
    WindowAggregation,
)


logger = logging.getLogger(__name__)


class HBaseSink:

    def __init__(
        self,
        host: str = "hbase",
        port: int = 16000,
        namespace: str = "ecommerce",
        table_name: str = "realtime_alerts",
        column_family: str = "cf",
        connection_timeout_ms: int = 5000,
    ) -> None:
        if not host.strip():
            raise ValueError(
                "host não pode ser vazio."
            )

        if port <= 0:
            raise ValueError(
                "port deve ser maior que zero."
            )

        if not namespace.strip():
            raise ValueError(
                "namespace não pode ser vazio."
            )

        if not table_name.strip():
            raise ValueError(
                "table_name não pode ser vazio."
            )

        if not column_family.strip():
            raise ValueError(
                "column_family não pode ser vazio."
            )

        if connection_timeout_ms <= 0:
            raise ValueError(
                "connection_timeout_ms deve ser maior que zero."
            )

        self.host = host
        self.port = port
        self.namespace = namespace
        self.table_name = table_name
        self.column_family = column_family
        self.connection_timeout_ms = connection_timeout_ms

        self._connection: Any = None
        self._table: Any = None

    @property
    def qualified_table_name(self) -> str:
        return (
            f"{self.namespace}:"
            f"{self.table_name}"
        )

    @property
    def connected(self) -> bool:
        return (
            self._connection is not None
            and self._table is not None
        )

    def connect(self) -> None:

        if self.connected:
            return

        try:
            import happybase

        except ImportError as exc:
            raise RuntimeError(
                "O sink HBase requer o pacote happybase."
            ) from exc

        self._connection = happybase.Connection(
            host=self.host,
            port=self.port,
            timeout=self.connection_timeout_ms,
            autoconnect=True,
        )

        self._table = self._connection.table(
            self.qualified_table_name
        )

        logger.info(
            "Conexão HBase estabelecida: %s",
            self.qualified_table_name,
        )

    def close(self) -> None:

        if self._connection is not None:
            self._connection.close()

        self._connection = None
        self._table = None

        logger.info(
            "Conexão HBase encerrada."
        )

    def build_row_key(
        self,
        alert: AlertEvent,
    ) -> str:

        if not isinstance(alert, AlertEvent):
            raise TypeError(
                "alert deve ser uma instância de AlertEvent."
            )

        return (
            f"{alert.event_timestamp.timestamp():.6f}"
            f"#{alert.alert_type}"
            f"#{alert.alert_id}"
        )

    def serialize_alert(
        self,
        alert: AlertEvent,
    ) -> Dict[bytes, bytes]:

        if not isinstance(alert, AlertEvent):
            raise TypeError(
                "alert deve ser uma instância de AlertEvent."
            )

        payload = alert.to_dict()

        return {
            b"cf:alert_id": (
                alert.alert_id.encode("utf-8")
            ),
            b"cf:alert_type": (
                alert.alert_type.encode("utf-8")
            ),
            b"cf:severity": (
                alert.severity.encode("utf-8")
            ),
            b"cf:message": (
                alert.message.encode("utf-8")
            ),
            b"cf:event_type": (
                str(
                    alert.event_type or ""
                ).encode("utf-8")
            ),
            b"cf:customer_id": (
                str(
                    alert.customer_id or ""
                ).encode("utf-8")
            ),
            b"cf:product_id": (
                str(
                    alert.product_id or ""
                ).encode("utf-8")
            ),
            b"cf:detected_at": (
                alert.detected_at.isoformat()
                .encode("utf-8")
            ),
            b"cf:event_timestamp": (
                alert.event_timestamp.isoformat()
                .encode("utf-8")
            ),
            b"cf:metrics": (
                json.dumps(
                    payload["metrics"],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            ),
        }

    def write_alert(
        self,
        alert: AlertEvent,
    ) -> str:

        self.connect()

        row_key = self.build_row_key(alert)

        data = self.serialize_alert(alert)

        self._table.put(
            row_key.encode("utf-8"),
            data,
        )

        logger.info(
            "Alerta persistido no HBase: row_key=%s",
            row_key,
        )

        return row_key

    def write_alerts(
        self,
        alerts: Iterable[AlertEvent],
    ) -> int:

        alerts_list = list(alerts)

        if not alerts_list:
            return 0

        self.connect()

        count = 0

        with self._table.batch(
            batch_size=100
        ) as batch:
            for alert in alerts_list:
                row_key = self.build_row_key(
                    alert
                )

                batch.put(
                    row_key.encode("utf-8"),
                    self.serialize_alert(alert),
                )

                count += 1

        logger.info(
            "%d alertas persistidos no HBase.",
            count,
        )

        return count

    def write_aggregation(
        self,
        aggregation: WindowAggregation,
    ) -> str:

        if not isinstance(
            aggregation,
            WindowAggregation,
        ):
            raise TypeError(
                "aggregation deve ser uma instância "
                "de WindowAggregation."
            )

        self.connect()

        key = (
            f"window#"
            f"{aggregation.window_start.timestamp():.6f}"
            f"#"
            f"{aggregation.event_type}"
            f"#"
            f"{aggregation.key or 'global'}"
        )

        data = {
            b"cf:alert_id": key.encode(
                "utf-8"
            ),
            b"cf:alert_type": (
                b"WINDOW_AGGREGATION"
            ),
            b"cf:severity": b"INFO",
            b"cf:event_type": (
                aggregation.event_type.encode(
                    "utf-8"
                )
            ),
            b"cf:event_timestamp": (
                aggregation.window_end.isoformat()
                .encode("utf-8")
            ),
            b"cf:metrics": (
                json.dumps(
                    aggregation.to_dict(),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            ),
        }

        self._table.put(
            key.encode("utf-8"),
            data,
        )

        return key

    def healthcheck(self) -> bool:

        try:
            self.connect()
            self._connection.tables()
            return True

        except Exception as exc:
            logger.warning(
                "Healthcheck HBase falhou: %s",
                exc,
            )
            return False

    def describe(self) -> Dict[str, Any]:
        return {
            "type": "hbase",
            "host": self.host,
            "port": self.port,
            "namespace": self.namespace,
            "table": self.table_name,
            "qualified_table": (
                self.qualified_table_name
            ),
            "column_family": self.column_family,
            "connected": self.connected,
        }