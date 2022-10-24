from faker import Faker
from dataclasses import dataclass, asdict

@dataclass
class User:
    name: str
    address: str
    year_of_birth: str
    
    def dict(self):
        return {k: v for k, v in asdict(self).items()}
    

@dataclass
class Order:
    user: str
    sale_id: str
    status: str
    transaction_id: str = None
    
    def dict(self):
        return {k: v for k, v in asdict(self).items()}
    
    
def get_registered_user():
    fake = Faker()
    user = User(fake.name(), fake.address(), fake.year())
    return user.dict()
    
    
fake = Faker()
print(fake.sha256())



