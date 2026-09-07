from __future__ import annotations

import logging
from typing import Iterable, List, Optional

from src.streaming.models import (
    AlertEvent,
    StreamingEvent,
    WindowAggregation,
)

from .alert_rules import AlertRule


logger = logging.getLogger(__name__)


class AlertDetector:
    
    def __init__(
        self,
        rules: Optional[
            Iterable[AlertRule]
        ] = None,
    ) -> None:
        self.rules: List[AlertRule] = []

        if rules:
            for rule in rules:
                self.register_rule(rule)

    def register_rule(
        self,
        rule: AlertRule,
    ) -> None:
        if not isinstance(rule, AlertRule):
            raise TypeError(
                "rule deve ser uma instância de AlertRule."
            )

        self.rules.append(rule)

        logger.debug(
            "Regra de alerta registrada: %s",
            rule.alert_type,
        )

    def detect(
        self,
        event: StreamingEvent,
        aggregation: Optional[
            WindowAggregation
        ] = None,
    ) -> List[AlertEvent]:
        """
        Avalia todas as regras sobre um evento.
        """

        if not isinstance(event, StreamingEvent):
            raise TypeError(
                "event deve ser uma instância de StreamingEvent."
            )

        alerts: List[AlertEvent] = []

        for rule in self.rules:
            try:
                alert = rule.evaluate(
                    event,
                    aggregation,
                )

                if alert is not None:
                    alerts.append(alert)

            except (TypeError, ValueError) as exc:
                logger.error(
                    "Falha ao avaliar regra %s: %s",
                    rule.alert_type,
                    exc,
                )

        return alerts

    def detect_batch(
        self,
        events: Iterable[StreamingEvent],
    ) -> List[AlertEvent]:

        alerts: List[AlertEvent] = []

        for event in events:
            alerts.extend(
                self.detect(event)
            )

        return alerts

    def detect_window(
        self,
        events: Iterable[StreamingEvent],
        aggregation: WindowAggregation,
    ) -> List[AlertEvent]:

        event_list = list(events)

        alerts: List[AlertEvent] = []

        for event in event_list:
            alerts.extend(
                self.detect(
                    event,
                    aggregation,
                )
            )

        return alerts

    def has_alerts(
        self,
        event: StreamingEvent,
        aggregation: Optional[
            WindowAggregation
        ] = None,
    ) -> bool:
        return bool(
            self.detect(
                event,
                aggregation,
            )
        )

    def count_alerts(
        self,
        events: Iterable[StreamingEvent],
    ) -> int:
        return len(
            self.detect_batch(events)
        )

    def clear_rules(self) -> None:
        self.rules.clear()

    def describe(self) -> dict[str, object]:
        return {
            "rules_count": len(self.rules),
            "rules": [
                {
                    "alert_type": rule.alert_type,
                    "severity": rule.severity,
                    "description": rule.description,
                }
                for rule in self.rules
            ],
        }