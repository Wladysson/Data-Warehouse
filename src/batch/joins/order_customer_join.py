from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class OrderCustomerJoinConfig:

    order_key: str = "customer_id"
    customer_key: str = "customer_id"
    join_type: str = "left"

    def validate(self) -> None:
        if not self.order_key.strip():
            raise ValueError(
                "order_key não pode ser vazio."
            )

        if not self.customer_key.strip():
            raise ValueError(
                "customer_key não pode ser vazio."
            )

        allowed_join_types = {
            "inner",
            "left",
            "right",
            "outer",
            "left_semi",
            "left_anti",
        }

        if self.join_type not in allowed_join_types:
            raise ValueError(
                f"join_type inválido: {self.join_type}. "
                f"Valores permitidos: {sorted(allowed_join_types)}."
            )


class OrderCustomerJoin:

    def __init__(
        self,
        config: Optional[OrderCustomerJoinConfig] = None,
    ) -> None:
        self.config = config or OrderCustomerJoinConfig()
        self.config.validate()

    def validate_dataframe(
        self,
        dataframe: Any,
        dataframe_name: str,
    ) -> None:
        if dataframe is None:
            raise ValueError(
                f"{dataframe_name} não pode ser None."
            )

        if not dataframe.columns:
            raise ValueError(
                f"{dataframe_name} não possui colunas."
            )

    def validate_join_keys(
        self,
        orders: Any,
        customers: Any,
    ) -> None:
        self.validate_dataframe(
            orders,
            "orders",
        )

        self.validate_dataframe(
            customers,
            "customers",
        )

        if self.config.order_key not in orders.columns:
            raise ValueError(
                f"Chave '{self.config.order_key}' "
                "não encontrada no DataFrame de pedidos."
            )

        if self.config.customer_key not in customers.columns:
            raise ValueError(
                f"Chave '{self.config.customer_key}' "
                "não encontrada no DataFrame de clientes."
            )

    def filter_valid_orders(
        self,
        orders: Any,
    ) -> Any:
        if orders is None:
            raise ValueError(
                "orders não pode ser None."
            )

        from pyspark.sql import functions as F

        return orders.filter(
            F.col(self.config.order_key).isNotNull()
        )

    def filter_valid_customers(
        self,
        customers: Any,
    ) -> Any:
        if customers is None:
            raise ValueError(
                "customers não pode ser None."
            )

        from pyspark.sql import functions as F

        return customers.filter(
            F.col(self.config.customer_key).isNotNull()
        )

    def prepare_customer_columns(
        self,
        customers: Any,
    ) -> Any:
        if customers is None:
            raise ValueError(
                "customers não pode ser None."
            )

        protected_columns = {
            self.config.customer_key,
        }

        renamed = customers

        for column in customers.columns:
            if column in protected_columns:
                continue

            if column in {
                "event_id",
                "event_type",
                "event_timestamp",
                "ingestion_timestamp",
                "session_id",
                "product_id",
                "order_id",
                "cart_id",
                "delivery_id",
                "quantity",
                "unit_price",
                "total_amount",
                "status",
            }:
                renamed = renamed.withColumnRenamed(
                    column,
                    f"customer_{column}",
                )

        return renamed

    def join(
        self,
        orders: Any,
        customers: Any,
    ) -> Any:
        self.validate_join_keys(
            orders,
            customers,
        )

        valid_orders = self.filter_valid_orders(
            orders
        )

        valid_customers = self.filter_valid_customers(
            customers
        )

        prepared_customers = self.prepare_customer_columns(
            valid_customers
        )

        result = valid_orders.join(
            prepared_customers,
            valid_orders[self.config.order_key]
            == prepared_customers[self.config.customer_key],
            self.config.join_type,
        )

        logger.info(
            "JOIN pedidos × clientes executado: "
            "tipo=%s, chave_pedido=%s, chave_cliente=%s.",
            self.config.join_type,
            self.config.order_key,
            self.config.customer_key,
        )

        return result

    def join_with_customer_alias(
        self,
        orders: Any,
        customers: Any,
    ) -> Any:
        self.validate_join_keys(
            orders,
            customers,
        )

        from pyspark.sql import functions as F

        order_alias = orders.alias("orders")
        customer_alias = customers.alias("customers")

        return order_alias.join(
            customer_alias,
            F.col(
                f"orders.{self.config.order_key}"
            )
            == F.col(
                f"customers.{self.config.customer_key}"
            ),
            self.config.join_type,
        )

    def find_orders_without_customer(
        self,
        orders: Any,
        customers: Any,
    ) -> Any:
        self.validate_join_keys(
            orders,
            customers,
        )

        valid_orders = self.filter_valid_orders(
            orders
        )

        valid_customers = self.filter_valid_customers(
            customers
        )

        return valid_orders.join(
            valid_customers.select(
                self.config.customer_key
            ).dropDuplicates(),
            valid_orders[self.config.order_key]
            == valid_customers[self.config.customer_key],
            "left_anti",
        )

    def find_customers_without_orders(
        self,
        orders: Any,
        customers: Any,
    ) -> Any:
        self.validate_join_keys(
            orders,
            customers,
        )

        valid_orders = self.filter_valid_orders(
            orders
        )

        valid_customers = self.filter_valid_customers(
            customers
        )

        return valid_customers.join(
            valid_orders.select(
                self.config.order_key
            ).dropDuplicates(),
            valid_customers[self.config.customer_key]
            == valid_orders[self.config.order_key],
            "left_anti",
        )

    def create_customer_sales_summary(
        self,
        joined_dataframe: Any,
    ) -> Any:
        if joined_dataframe is None:
            raise ValueError(
                "joined_dataframe não pode ser None."
            )

        from pyspark.sql import functions as F

        if "customer_id" not in joined_dataframe.columns:
            raise ValueError(
                "O DataFrame precisa possuir customer_id."
            )

        aggregations = [
            F.count("*").alias(
                "order_count"
            ),
        ]

        if "quantity" in joined_dataframe.columns:
            aggregations.append(
                F.sum("quantity").alias(
                    "total_items"
                )
            )

        if "total_amount" in joined_dataframe.columns:
            aggregations.extend(
                [
                    F.round(
                        F.sum("total_amount"),
                        2,
                    ).alias(
                        "total_sales"
                    ),
                    F.round(
                        F.avg("total_amount"),
                        2,
                    ).alias(
                        "average_order_value"
                    ),
                ]
            )

        return (
            joined_dataframe
            .groupBy("customer_id")
            .agg(*aggregations)
        )

    def run(
        self,
        orders: Any,
        customers: Any,
    ) -> Any:
        return self.join(
            orders=orders,
            customers=customers,
        )

    def describe(self) -> dict[str, Any]:
        return {
            "order_key": self.config.order_key,
            "customer_key": self.config.customer_key,
            "join_type": self.config.join_type,
            "operation": "orders_to_customers",
            "distributed_processing": True,
        }