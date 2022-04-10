from decimal import Decimal

from pony.orm import *
import time
# from models import Warehouse, Item, Stock

setting = {
    "provider": "postgres",
    "host": "127.0.0.1",
    "database": "pony_demo",
    "user": "jinyuhan",
    "password": "761513134",
}
db = Database(**setting)
class Warehouse(db.Entity):
    _table_ = "warehouse"
    w_id = PrimaryKey(int, auto=True)
    w_name = Required(str, max_len=50)
    w_street = Required(str, max_len=50)
    w_city = Required(str, max_len=50)
    w_country = Required(str, max_len=50)
    stock = Set("Stock")

class Item(db.Entity):
    _table_ = "items"
    i_id = PrimaryKey(int, auto=True)
    i_im_id = Required(str, max_len=8)
    i_name = Required(str, max_len=50)
    i_price = Required(Decimal)
    stock = Set("Stock")

class Stock(db.Entity):
    _table_ = "stocks"
    # warehouse = Required(Warehouse, nullable=True, column="w_id", reverse="stock")
    w_id = Required(Warehouse)
    i_id = Required(Item)
    # item = Required(Item, nullable=True, column="i_id", reverse="stock")
    s_qty = Required(int)
    PrimaryKey(w_id, i_id)


db.generate_mapping(create_tables=False)
#select warehouse join stocks by w_city
count = 0
cities = [
    'Santa Cruz',
    'Washington',
    'Bibrka',
    'Haninge',
    'Inta'
]
start = time.time()
for _ in range(100):
    with db_session:
        for city in cities:
            # res = list(select(s for s in Stock for w in s.w_id if w.w_city == city))
            res = list(select(s for s in Stock if s.w_id.w_city == city))
        count += len(res)
now = time.time()
print(count)
print(f"Pony ORM, select warehouse join stocks by w_city: Rows/sec: {count / (now - start): .2f}")



#select warehouse join stocks by w_city and item
count = 0
city_iname = [
    ('Stockholm', 'Pedi-Dri'),
    ('Hesheng', 'Glyburide'),
    ('Ciekek', 'Hydrocortisone'),
    ('Oslo', 'ACNE SOLUTIONS'),
    ('Washington', 'Doxycycline Hyclate')
]
start = time.time()

for _ in range(100):
    with db_session:
        for city, i_name in city_iname:
            res = list(select(s for s in select(s for s in Stock for w in s.w_id if w.w_city == city) for i in s.i_id if i.i_name == i_name))
            # res = list(select(s1 for w in Warehouse for i in Item for s1 in i.stock for s2 in w.stock if w.w_city == city and i.i_name == i_name and s1 == s2))
            count += len(res)
now = time.time()
print(count)
print(f"Pony ORM, select warehouse join stocks by w_city and item: Rows/sec: {count / (now - start): .2f}")


#update whole
start = time.time()
count = 0
with db_session:
    count = len(list(select(w for w in Warehouse)))
    for i in range(1, count+1):
        w = Warehouse[i]
        w.w_name = f"{w.w_name} Update"
        w.w_street = f"{w.w_street} Update"
        w.w_city = f"{w.w_city} Update"
        w.w_country = f"{w.w_country} Update"
now = time.time()
print(f"Pony ORM, update whole object: Rows/sec: {count / (now - start): .2f}")

#update single column
start = time.time()
with db_session:
    for i in range(1, count + 1):
        w = Warehouse[i]
        w.w_country = f"{w.w_country} Update2"
now = time.time()
print(f"Pony ORM, update single column: Rows/sec: {count / (now - start): .2f}")

#Delete
start = time.time()
with db_session:
    for i in range(1, count + 1):
        w = Warehouse[i]
        w.delete()
now = time.time()
print(f"Pony ORM, delete: Rows/sec: {count / (now - start): .2f}")




