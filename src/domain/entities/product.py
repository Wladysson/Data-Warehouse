from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Product:

    product_id: str
    name: str
    category_id: str
    price: float
    stock_quantity: int = 0
    brand: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.product_id.strip():
            raise ValueError(
                "product_id não pode ser vazio."
            )

        if not self.name.strip():
            raise ValueError(
                "name não pode ser vazio."
            )

        if not self.category_id.strip():
            raise ValueError(
                "category_id não pode ser vazio."
            )

        if self.price < 0:
            raise ValueError(
                "price não pode ser negativo."
            )

        if self.stock_quantity < 0:
            raise ValueError(
                "stock_quantity não pode ser negativo."
            )

        if self.brand is not None and not self.brand.strip():
            raise ValueError(
                "brand não pode ser vazia quando informada."
            )

    @property
    def is_in_stock(self) -> bool:

        return self.stock_quantity > 0

    @property
    def is_out_of_stock(self) -> bool:

        return self.stock_quantity == 0

    @property
    def is_high_value(self) -> bool:

        return self.price >= 1000.0

    def to_dict(self) -> dict[str, Any]:

        return {
            "product_id": self.product_id,
            "name": self.name,
            "category_id": self.category_id,
            "price": self.price,
            "stock_quantity": self.stock_quantity,
            "brand": self.brand,
            "metadata": dict(self.metadata),
        }