from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class Customer:
    
    customer_id: str
    name: str
    email: str
    registration_date: datetime
    segment: str = "standard"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.customer_id.strip():
            raise ValueError(
                "customer_id não pode ser vazio."
            )

        if not self.name.strip():
            raise ValueError(
                "name não pode ser vazio."
            )

        if not self.email.strip():
            raise ValueError(
                "email não pode ser vazio."
            )

        if "@" not in self.email:
            raise ValueError(
                "email deve possuir um formato válido."
            )

        if self.registration_date.tzinfo is None:
            raise ValueError(
                "registration_date deve possuir timezone."
            )

        if not self.segment.strip():
            raise ValueError(
                "segment não pode ser vazio."
            )

    @property
    def normalized_email(self) -> str:

        return self.email.strip().lower()

    @property
    def is_premium(self) -> bool:

        return self.segment.lower() in {
            "premium",
            "vip",
            "enterprise",
        }

    def to_dict(self) -> dict[str, Any]:

        return {
            "customer_id": self.customer_id,
            "name": self.name,
            "email": self.normalized_email,
            "registration_date": self.registration_date.isoformat(),
            "segment": self.segment,
            "metadata": dict(self.metadata),
        }