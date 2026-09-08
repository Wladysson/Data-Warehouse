from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ClickTransformationConfig:
    event_type: str = "click"

    def validate(self) -> None:
        if not self.event_type.strip():
            raise ValueError("event_type não pode ser vazio.")


class ClickTransformations:

    def __init__(
        self,
        config: Optional[ClickTransformationConfig] = None,
    ) -> None:
        self.config = config or ClickTransformationConfig()
        self.config.validate()

    def filter_clicks(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "event_type" not in dataframe.columns:
            raise ValueError(
                "O DataFrame precisa possuir a coluna event_type."
            )

        from pyspark.sql import functions as F

        return dataframe.filter(
            F.col("event_type") == self.config.event_type
        )

    def normalize_action(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "action" not in dataframe.columns:
            logger.warning(
                "Coluna action não encontrada; normalização ignorada."
            )
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "action",
            F.lower(
                F.trim(
                    F.col("action")
                )
            ),
        )

    def classify_click(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "action" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "click_category",
            F.when(
                F.col("action") == "search",
                "search",
            )
            .when(
                F.col("action") == "view",
                "navigation",
            )
            .when(
                F.col("action") == "product_view",
                "product_interest",
            )
            .when(
                F.col("action") == "add_to_cart",
                "conversion_intent",
            )
            .otherwise("other"),
        )

    def add_product_interaction_flag(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "action" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "is_product_interaction",
            F.when(
                F.col("action").isin(
                    "product_view",
                    "add_to_cart",
                ),
                1,
            ).otherwise(0),
        )

    def add_conversion_intent_flag(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "action" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        return dataframe.withColumn(
            "has_conversion_intent",
            F.when(
                F.col("action") == "add_to_cart",
                1,
            ).otherwise(0),
        )

    def add_session_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        from pyspark.sql import functions as F

        result = dataframe

        if "session_id" in result.columns:
            result = result.withColumn(
                "has_session",
                F.when(
                    F.col("session_id").isNotNull()
                    & (
                        F.length(
                            F.trim(
                                F.col("session_id")
                            )
                        ) > 0
                    ),
                    1,
                ).otherwise(0),
            )

        return result

    def add_temporal_features(
        self,
        dataframe: Any,
    ) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        if "event_timestamp" not in dataframe.columns:
            return dataframe

        from pyspark.sql import functions as F

        result = dataframe

        result = result.withColumn(
            "click_hour",
            F.hour(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "click_day_of_week",
            F.dayofweek(F.col("event_timestamp")),
        )

        result = result.withColumn(
            "click_is_weekend",
            F.when(
                F.dayofweek(
                    F.col("event_timestamp")
                ).isin(1, 7),
                1,
            ).otherwise(0),
        )

        return result

    def transform(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        result = self.filter_clicks(dataframe)
        result = self.normalize_action(result)
        result = self.classify_click(result)
        result = self.add_product_interaction_flag(result)
        result = self.add_conversion_intent_flag(result)
        result = self.add_session_features(result)
        result = self.add_temporal_features(result)

        logger.info(
            "Transformações de eventos de clique concluídas."
        )

        return result

    def create_summary(self, dataframe: Any) -> Any:
        if dataframe is None:
            raise ValueError("dataframe não pode ser None.")

        transformed = self.transform(dataframe)

        from pyspark.sql import functions as F

        group_columns = []

        for column in (
            "event_date",
            "product_id",
            "action",
            "click_category",
        ):
            if column in transformed.columns:
                group_columns.append(column)

        if not group_columns:
            raise ValueError(
                "Nenhuma coluna válida para agrupamento foi encontrada."
            )

        aggregations = [
            F.count("*").alias("click_count"),
        ]

        if "customer_id" in transformed.columns:
            aggregations.append(
                F.countDistinct(
                    "customer_id"
                ).alias("unique_customers")
            )

        if "session_id" in transformed.columns:
            aggregations.append(
                F.countDistinct(
                    "session_id"
                ).alias("unique_sessions")
            )

        return (
            transformed
            .groupBy(*group_columns)
            .agg(*aggregations)
        )

    def describe(self) -> dict[str, Any]:
        return {
            "event_type": self.config.event_type,
            "transformation": "click",
            "features": [
                "click_category",
                "is_product_interaction",
                "has_conversion_intent",
                "has_session",
                "click_hour",
                "click_day_of_week",
                "click_is_weekend",
            ],
        }