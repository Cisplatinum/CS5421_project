from decimal import Decimal

from pony.orm import *
import json
import time



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
    w_name = Required(str)
    w_street = Required(str)
    w_city = Required(str)
    w_country = Required(str)
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
    # PrimaryKey(warehouse, item)
    # s_qty = Required(int)


class User(db.Entity):
    _table_ = "users"
    u_id = PrimaryKey(int, auto=True)
    firstname = Required(str)
    lastname = Required(str)
    email = Required(str)
    gender = Required(str)
    ip = Required(str)
    hometown_city = Required(str)
    contact_number = Required(str)
    ssn = Required(str)
    credit_card = Required(str)
    credit_card_type = Required(str)
    job_title = Required(str)
    university = Required(str)
    linkedin_skills = Required(str)



if __name__ == "__main__":
    db.drop_table(table_name="warehouse", if_exists=True, with_all_data=True)  # 删除表，演示实体声明时用于快速清除旧表
    db.drop_table(table_name="items", if_exists=True, with_all_data=True)
    db.drop_table(table_name="stocks", if_exists=True, with_all_data=True)
    db.drop_table(table_name="users", if_exists=True, with_all_data=True)
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

    #bulk insert
    start = time.time()
    BATCH_SIZE = 50
    for i in range(0, num_rows, BATCH_SIZE):
        with db_session:
            for j in range(i, i+BATCH_SIZE):
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
    print(f"Pony ORM, BATCH {BATCH_SIZE} Insert: Rows/sec: {num_rows / (now - start): .2f}")

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
    # with db_session:
    #     for _ in range(100):
    #         for city in cities:
    #             res = list(select(w for w in Warehouse if w.w_city == city))
    #             count += len(res)
    for _ in range(100):
        with db_session:
            for city in cities:
                res = list(select(w for w in Warehouse if w.w_city == city))
                count += len(res)
    now = time.time()
    print(f"Pony ORM, select by w_city: Rows/sec: {count / (now - start): .2f}")

    #Filter - slect by w_city and street
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

    # Filter - select by w_city limit 20
    count = 0
    start = time.time()
    for _ in range(100):
        with db_session:
            for city in cities:
                res = list(select(w for w in Warehouse if w.w_city == city).limit(20))
                count += len(res)
    now = time.time()
    print(f"Pony ORM, select by w_city with limit 20: Rows/sec: {count / (now - start): .2f}")

    #Dicts
    count = 0
    start = time.time()
    for _ in range(100):
        with db_session:
            for city in cities:
                res = [obj.to_dict() for obj in select(w for w in Warehouse if w.w_city == city)]
                count += len(res)
    now = time.time()
    print(f"Pony ORM, select by w_city transform to dict: Rows/sec: {count / (now - start): .2f}")




# big table
with open('bigtable.json', 'r') as f:
    users = json.loads(f.read())
num_user_rows = len(users)

#single insert
start = time.time()
for i in range(num_user_rows):
    u = users[i]
    with db_session:
        User(
            firstname=u['first_name'],
            lastname=u['last_name'],
            email=u['email'],
            gender=u['gender'],
            ip=u['ip_address'],
            hometown_city=u['hometown_city'],
            contact_number=u['contact_number'],
            ssn=u['ssn'],
            credit_card=u['credit_card'],
            credit_card_type=u['credit_card_type'],
            job_title=u['job_title'],
            university=u['university'],
            linkedin_skills=u['Linkedin_skills']
        )
        commit()
now = time.time()
print(f"Pony ORM, Bigtable Single Insert: Rows/sec: {num_user_rows / (now - start): .2f}")

#bulk insert
start = time.time()
for i in range(0, num_user_rows, BATCH_SIZE):
    with db_session:
        for j in range(i, i+BATCH_SIZE):
            u = users[j]
            User(
                firstname=u['first_name'],
                lastname=u['last_name'],
                email=u['email'],
                gender=u['gender'],
                ip=u['ip_address'],
                hometown_city=u['hometown_city'],
                contact_number=u['contact_number'],
                ssn=u['ssn'],
                credit_card=u['credit_card'],
                credit_card_type=u['credit_card_type'],
                job_title=u['job_title'],
                university=u['university'],
                linkedin_skills=u['Linkedin_skills']
            )
        commit()
now = time.time()
print(f"Pony ORM, Bigtable BATCH {BATCH_SIZE} Insert: Rows/sec: {num_user_rows / (now - start): .2f}")

#Single select by city
count = 0
genders = ['Female','Polygender']
cities = ['Nanjing', 'Wudui', 'Midlands']
# cities = [
#     'Stockholm',
#     'Oslo',
#     'Buenavista',
#     'San Antonio',
#     'Washington'
# ]
start = time.time()
for _ in range(100):
    with db_session:
        for city in cities:
            res = list(select(u for u in User if u.hometown_city == city))
            count += len(res)
now = time.time()
print(f"Pony ORM, select by city: Rows/sec: {count / (now - start): .2f}")

# Experiment 2b: Single select by gender and city
count = 0
city_gender = [
    ('Nanjing', 'Female'),
    ('Wudui', 'Male'),
    ('Midlands', 'Polygender')
]

start = time.time()

for _ in range(100):
    with db_session:
        for city, gender in city_gender:
            res = list(select(u for u in User if u.hometown_city == city and u.gender == gender))
            count += len(res)

now = time.time()
print(f"Pony ORM, select by gender and city: Rows/sec: {count / (now - start): .2f}")


# Filter - select by city limit 20
count = 0
genders = ['Female','Polygender']
cities = ['Nanjing', 'Wudui', 'Midlands']
start = time.time()
for _ in range(100):
    with db_session:
        for city in cities:
            res = list(select(u for u in User if u.hometown_city == city).limit(20))
            count += len(res)
now = time.time()
print(f"Pony ORM, select by city with limit 20: Rows/sec: {count / (now - start): .2f}")

#dict
count = 0
genders = ['Female', 'Polygender']
start = time.time()
for _ in range(100):
    with db_session:
        for gender in genders:
            res = [obj.to_dict() for obj in select(u for u in User if u.gender == gender)]
            count += len(res)
now = time.time()
print(f"Pony ORM, select by gender transform to dict: Rows/sec: {count / (now - start): .2f}")







# Experiment 3a. Update whole
start = time.time()
count = num_user_rows
with db_session:
    # count = len(list(select(u for u in User)))
    for i in range(1, count+1):
        obj = User[i]
        obj.firstname = f"{obj.firstname} Update"
        obj.lastname = f"{obj.lastname} Update"
        obj.email = f"{obj.email} Update"
        obj.gender = f"{obj.gender} Update"
        obj.ip = f"{obj.ip} Update"
        obj.hometown_city = f"{obj.hometown_city} Update"
        obj.contact_number = f"{obj.contact_number} Update"
        obj.ssn = f"{obj.ssn} Update"
        obj.credit_card = f"{obj.credit_card} Update"
        obj.credit_card_type = f"{obj.credit_card_type} Update"
        obj.job_title = f"{obj.job_title} Update"
        obj.university = f"{obj.university} Update"
        obj.linkedin_skills = f"{obj.linkedin_skills} Update"
now = time.time()
print(f"Pony ORM, Bigtable update whole object: Rows/sec: {count / (now - start): .2f}")


# Experiment 3b. Update partial (update only a single field city)
start = time.time()
with db_session:
    for i in range(1, count + 1):
        obj = User[i]
        obj.hometown_city= f"{obj.hometown_city} Update2"
now = time.time()
print(f"Pony ORM, update single column: Rows/sec: {count / (now - start): .2f}")

#Delete
start = time.time()
with db_session:
    for i in range(1, count + 1):
        u = User[i]
        u.delete()
now = time.time()
print(f"Pony ORM, Bigtable delete: Rows/sec: {count / (now - start): .2f}")










