import time
from model_big import Bigtable, engine
from sqlalchemy.orm import sessionmaker

count = 0
cities = ['Nanjing', 'Wudui', 'Midlands']

Session = sessionmaker(bind=engine)
start = now = time.time()
session = Session()

# single select by city
for _ in range(100):
    for city in cities:
        res = list(session.query(Bigtable).filter(Bigtable.hometown_city == city).limit(20))
        count += len(res)

session.commit()
now = time.time()

print(f"SQLAlchemy ORM, select by city limit 20 bigtable: Rows/sec: {count / (now - start): 10.2f}")
