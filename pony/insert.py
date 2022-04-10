#load data from json file
import json
import time
from pony.orm import *
from models import Warehouse


with open('warehouse.json','r') as f:
    warehouses = json.loads(f.read())

num_rows = len(warehouses)

#single insert
start = time.time()

for i in range(num_rows):
    wh = warehouses[i]
    with db_session:
        Warehouse(
            w_id = wh['w_id'],
            w_name = wh['w_name'],
            w_street = wh['w_street'],
            w_city = wh['w_city'],
            w_country = wh['w_country']
        )
        commit()
now = time.time()
print(f"Pony ORM, A: Rows/sec: {count / (now - start): 10.2f}")





