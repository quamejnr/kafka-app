from faker import Faker
from dataclasses import dataclass, asdict    

@dataclass
class Order:
    user: str
    sale_id: str
    status: str
    transaction_id: str = None
    
    def dict(self):
        return {k: v for k, v in asdict(self).items()}




