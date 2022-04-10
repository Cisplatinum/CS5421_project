import time

from model import Warehouse, engine
from sqlalchemy.orm import sessionmaker

count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']

Session = sessionmaker(bind=engine)
start = now = time.time()
session = Session()

# single select by city
for _ in range(100):
    for city in cities:
        res = list(session.query(Warehouse).filter(Warehouse.w_city == city))
        count += len(res)

session.commit()
now = time.time()

print(f"SQLAlchemy ORM, select by city: Rows/sec: {count / (now - start): 10.2f}")
