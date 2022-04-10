import json
import time

from model import Warehouse, engine
from sqlalchemy.orm import sessionmaker

with open('warehouse.json', 'r') as f:
    warehouses = json.loads(f.read())

num_rows = len(warehouses)
batch_size = 50

Session = sessionmaker(bind=engine)
start = now = time.time()
session = Session()

# insert by batch
for i in range(0, num_rows, batch_size):
    session.add_all(
        [
            Warehouse(w_id=warehouses[j]['w_id'],
                      w_name=warehouses[j]['w_name'],
                      w_street=warehouses[j]['w_street'],
                      w_city=warehouses[j]['w_city'],
                      w_country=warehouses[j]['w_country']
                      ) for j in range(i, i + batch_size)
        ]
    )
session.commit()
now = time.time()

print(f"SQLAlchemy ORM, insert by batch: Rows/sec: {num_rows / (now - start): 10.2f}")
