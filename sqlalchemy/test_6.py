import time

from model import Warehouse, Stock, engine
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)

session = Session()

# update the whole object
count = 0
cities = [
    'Santa Cruz',
    'Washington',
    'Bibrka',
    'Haninge',
    'Inta'
]

start = now = time.time()

for _ in range(100):
    for city in cities:
        res = list(session.query(Stock).join(Warehouse).filter(Warehouse.w_city == city))
        count += len(res)

now = time.time()
print(count)
print(f"SQLAlchemy ORM, join filter by city: Rows/sec: {count / (now - start): 10.2f}")
