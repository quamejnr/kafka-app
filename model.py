from faker import Faker
from dataclasses import dataclass, asdict
from typing import Dict


@dataclass
class OrderEvent:
    user: str
    sale_id: str
    order_status: str
    transaction_id: str = None

    def dict(self) -> Dict[str, str]:
        return {k: v for k, v in asdict(self).items()}
