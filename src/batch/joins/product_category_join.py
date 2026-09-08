from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


_ALLOWED_JOIN_TYPES: Final[frozenset[str]] = frozenset(
    {"inner", "left", "right", "full", "left_outer", "right_outer", "full_outer"}
)


@dataclass(frozen=True, slots=True)
class ProductCategoryJoinConfig:
    
    product_key: str = "category_id"
    category_key: str = "category_id"
    join_type: str = "left"

    def __post_init__(self) -> None:
        if not self.product_key.strip():
            raise ValueError("product_key não pode ser vazio.")

        if not self.category_key.strip():
            raise ValueError("category_key não pode ser vazio.")

        if self.join_type not in _ALLOWED_JOIN_TYPES:
            raise ValueError(
                f"Tipo de JOIN inválido: {self.join_type}. "
                f"Valores permitidos: {sorted(_ALLOWED_JOIN_TYPES)}."
            )


class ProductCategoryJoin:

    def __init__(
        self,
        config: ProductCategoryJoinConfig | None = None,
    ) -> None:
        self.config = config or ProductCategoryJoinConfig()

    @staticmethod
    def _validate_dataframe(
        dataframe: DataFrame,
        name: str,
    ) -> None:
        if not isinstance(dataframe, DataFrame):
            raise TypeError(
                f"{name} deve ser uma instância de pyspark.sql.DataFrame."
            )

    @staticmethod
    def _validate_key(
        dataframe: DataFrame,
        key: str,
        name: str,
    ) -> None:
        if key not in dataframe.columns:
            raise ValueError(
                f"Chave '{key}' não encontrada no DataFrame de {name}."
            )

    def prepare_category_columns(
        self,
        categories: DataFrame,
    ) -> DataFrame:
        """
        Renomeia colunas da dimensão para evitar colisões durante o JOIN.
        """

        protected_columns = {
            self.config.category_key,
        }

        expressions = []

        for column in categories.columns:
            if column in protected_columns:
                expressions.append(F.col(column))
                continue

            if column.startswith("category_"):
                expressions.append(F.col(column))
            else:
                expressions.append(
                    F.col(column).alias(f"category_{column}")
                )

        return categories.select(*expressions)

    def join(
        self,
        products: DataFrame,
        categories: DataFrame,
    ) -> DataFrame:

        self._validate_dataframe(products, "products")
        self._validate_dataframe(categories, "categories")

        self._validate_key(
            products,
            self.config.product_key,
            "products",
        )

        self._validate_key(
            categories,
            self.config.category_key,
            "categories",
        )

        prepared_categories = self.prepare_category_columns(categories)

        return products.join(
            prepared_categories,
            products[self.config.product_key]
            == prepared_categories[self.config.category_key],
            self.config.join_type,
        )

    def join_with_category_alias(
        self,
        products: DataFrame,
        categories: DataFrame,
    ) -> DataFrame:

        self._validate_dataframe(products, "products")
        self._validate_dataframe(categories, "categories")

        self._validate_key(
            products,
            self.config.product_key,
            "products",
        )

        self._validate_key(
            categories,
            self.config.category_key,
            "categories",
        )

        product_alias = products.alias("product")
        category_alias = categories.alias("category")

        return product_alias.join(
            category_alias,
            F.col(
                f"product.{self.config.product_key}"
            )
            == F.col(
                f"category.{self.config.category_key}"
            ),
            self.config.join_type,
        )

    def find_products_without_category(
        self,
        products: DataFrame,
        categories: DataFrame,
    ) -> DataFrame:

        self._validate_dataframe(products, "products")
        self._validate_dataframe(categories, "categories")

        self._validate_key(
            products,
            self.config.product_key,
            "products",
        )

        self._validate_key(
            categories,
            self.config.category_key,
            "categories",
        )

        category_keys = categories.select(
            F.col(self.config.category_key).alias(
                "_category_join_key"
            )
        ).dropDuplicates()

        return products.join(
            category_keys,
            products[self.config.product_key]
            == category_keys["_category_join_key"],
            "left_anti",
        )

    def find_categories_without_products(
        self,
        products: DataFrame,
        categories: DataFrame,
    ) -> DataFrame:

        self._validate_dataframe(products, "products")
        self._validate_dataframe(categories, "categories")

        self._validate_key(
            products,
            self.config.product_key,
            "products",
        )

        self._validate_key(
            categories,
            self.config.category_key,
            "categories",
        )

        product_keys = products.select(
            F.col(self.config.product_key).alias(
                "_product_category_key"
            )
        ).dropDuplicates()

        return categories.join(
            product_keys,
            categories[self.config.category_key]
            == product_keys["_product_category_key"],
            "left_anti",
        )

    def create_category_product_summary(
        self,
        products: DataFrame,
        categories: DataFrame,
    ) -> DataFrame:

        enriched = self.join(products, categories)

        return (
            enriched.groupBy(self.config.product_key)
            .agg(
                F.count("*").alias("product_count"),
            )
            .orderBy(F.desc("product_count"))
        )

    def run(
        self,
        products: DataFrame,
        categories: DataFrame,
    ) -> DataFrame:
        """Executa o fluxo principal do JOIN."""

        return self.join(products, categories)

    def describe(self) -> dict[str, object]:
        """Retorna informações da configuração do JOIN."""

        return {
            "operation": "product_category_join",
            "product_key": self.config.product_key,
            "category_key": self.config.category_key,
            "join_type": self.config.join_type,
        }