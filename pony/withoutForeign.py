from decimal import Decimal

from pony.orm import *
import json
import time



setting = {
    "provider": "postgres",
    "host": "127.0.0.1",
    "database": "withoutForeign",
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

class Item(db.Entity):
    _table_ = "items"
    i_id = PrimaryKey(int, auto=True)
    i_im_id = Required(str, max_len=8)
    i_name = Required(str, max_len=50)
    i_price = Required(Decimal)

class Stock(db.Entity):
    _table_ = "stocks"
    w_id = Required(int)
    i_id = Required(int)
    s_qty = Required(int)
    PrimaryKey(w_id, i_id)


if __name__ == "__main__":
    db.drop_table(table_name="warehouse", if_exists=True, with_all_data=True)  # 删除表，演示实体声明时用于快速清除旧表
    db.drop_table(table_name="items", if_exists=True, with_all_data=True)
    db.drop_table(table_name="stocks", if_exists=True, with_all_data=True)
    db.generate_mapping(create_tables=True)  # 生成实体，表和映射关系
    with open('warehouse.json', 'r') as f:
        warehouses = json.loads(f.read())

    num_rows = len(warehouses)

    # single insert
    start = time.time()

    for i in range(num_rows):
        wh = warehouses[i]
        with db_session:
            Warehouse(
                w_id=wh['w_id'],
                w_name=wh['w_name'],
                w_street=wh['w_street'],
                w_city=wh['w_city'],
                w_country=wh['w_country']
            )
            commit()
    now = time.time()
    print(f"Pony ORM, Single Insert: Rows/sec: {num_rows / (now - start): .2f}")

    # Delete
    with db_session:
        for i in range(1, num_rows + 1):
            Warehouse[i].delete()

    # bulk insert
    start = time.time()
    BATCH_SIZE = 50
    for i in range(0, num_rows, BATCH_SIZE):
        with db_session:
            for j in range(i, BATCH_SIZE+i):
                wh = warehouses[j]
                Warehouse(
                    w_id=wh['w_id'],
                    w_name=wh['w_name'],
                    w_street=wh['w_street'],
                    w_city=wh['w_city'],
                    w_country=wh['w_country']
                )
            commit()
    now = time.time()
    print(f"Pony ORM, Bulk Insert: Rows/sec: {num_rows / (now - start): .2f}")


    #Filter - select by w_city
    count = 0
    cities = [
        'Stockholm',
        'Oslo',
        'Buenavista',
        'San Antonio',
        'Washington'
    ]
    start = time.time()

    for _ in range(100):
        with db_session:
            for city in cities:
                res = list(select(w for w in Warehouse if w.w_city == city))
                count += len(res)
    now = time.time()
    print(count)
    print(f"Pony ORM, select by w_city: Rows/sec: {count / (now - start): .2f}")

    #Filter - select by w_city and street
    count = 0
    addresses = [
        ('Nanjing', 'Starling'),
        ('Wudui', 'American'),
        ('Xiangyang', 'Bunker Hill'),
        ('Juntas', 'Bluestem'),
        ('Sinfra', 'Jenna')
    ]
    start = time.time()
    for _ in range(100):
        with db_session:
            for city, street in addresses:
                res = list(select(w for w in Warehouse if w.w_city == city and w.w_street == street))
                count += len(res)
    now = time.time()
    print(f"Pony ORM, select by w_city and w_street: Rows/sec: {count / (now - start): .2f}")

    #Filter - select by w_city limit 20
    count = 0
    start = time.time()
    for _ in range(100):
        with db_session:
            for city in cities:
                res = list(select(w for w in Warehouse if w.w_city == city).limit(20))
                count += len(res)
    now = time.time()
    print(f"Pony ORM, select by w_city limit 20: Rows/sec: {count / (now - start): .2f}")






