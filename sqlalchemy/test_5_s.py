import time

from model_small import WarehouseS, engine
from sqlalchemy.orm import sessionmaker

count = 0
addresses = [
    ('Nanjing', 'Starling'),
    ('Wudui', 'American'),
    ('Xiangyang', 'Bunker Hill'),
    ('Juntas', 'Bluestem'),
    ('Sinfra', 'Jenna')
]

Session = sessionmaker(bind=engine)
start = now = time.time()
session = Session()

# single select by city and street
for _ in range(100):
    for city, street in addresses:
        res = list(session.query(WarehouseS).filter(WarehouseS.w_city == city, WarehouseS.w_street == street))
        count += len(res)

session.commit()
now = time.time()

print(f"SQLAlchemy ORM, select by city and name small table: Rows/sec: {count / (now - start): 10.2f}")
