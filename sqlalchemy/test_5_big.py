import time
from model_big import Bigtable, engine
from sqlalchemy.orm import sessionmaker

count = 0
comb = [
    ('Nanjing', 'Male'),
    ('Wudui', 'Female'),
    ('Midlands', 'Polygender')
]

Session = sessionmaker(bind=engine)
start = now = time.time()
session = Session()

# single select by city and street
for _ in range(100):
    for i, j in comb:
        res = list(session.query(Bigtable).filter(Bigtable.hometown_city == i, Bigtable.gender == j))
        count += len(res)

session.commit()
now = time.time()

print(f"SQLAlchemy ORM, select by city and name bigtable: Rows/sec: {count / (now - start): 10.2f}")
