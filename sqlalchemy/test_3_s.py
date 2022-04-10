import json
import time

from model_small import WarehouseS, engine
from sqlalchemy.orm import sessionmaker

with open('warehouse.json', 'r') as f:
    warehouses = json.loads(f.read())

num_rows = len(warehouses)

Session = sessionmaker(bind=engine)
start = now = time.time()
session = Session()

# insert by bulk
session.bulk_save_objects([WarehouseS(w_id=wh['w_id'],
                                      w_name=wh['w_name'],
                                      w_street=wh['w_street'],
                                      w_city=wh['w_city'],
                                      w_country=wh['w_country']
                                      ) for wh in warehouses])
session.commit()
now = time.time()

print(f"SQLAlchemy ORM, insert by bulk small table: Rows/sec: {num_rows / (now - start): 10.2f}")
