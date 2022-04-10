from decimal import Decimal

from pony.orm import *
import time
import json
# from models import Warehouse, Item, Stock

setting = {
    "provider": "postgres",
    "host": "127.0.0.1",
    "database": "pony_demo",
    "user": "jinyuhan",
    "password": "761513134",
}
db = Database(**setting)

class User(db.Entity):
    _table_ = "users"
    u_id = PrimaryKey(int, auto=True)
    firstname = Required(str, max_len=50)
    lastname = Required(str, max_len=50)
    email = Required(str, max_len=50)
    gender = Required(str, max_len=50)
    ip = Required(str, max_len=50)
    hometown_city = Required(str, max_len=50)
    contact_number = Required(str, max_len=50)
    ssn = Required(str, max_len=50)
    credit_card = Required(str, max_len=50)
    credit_card_type = Required(str, max_len=50)
    job_title = Required(str, max_len=50)
    university = Required(str, max_len=50)
    linkedin_skills = Required(str, max_len=50)

db.generate_mapping(create_tables=True)

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
with db_session:
    for i in range(num_user_rows):
        u = users[i]
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
print(f"Pony ORM, Bigtable Bulk Insert: Rows/sec: {num_user_rows / (now - start): .2f}")

#Single select by gender
count = 0
genders = ['Female','Polygender']
start = time.time()
with db_session:
    for _ in range(100):
        for gender in genders:
            res = list(select(u for u in User if u.gender == gender))
            count += len(res)
now = time.time()
print(f"Pony ORM, select by gender: Rows/sec: {num_user_rows / (now - start): .2f}")

# Experiment 2b: Single select by gender and city
count = 0
city_gender = [
    ('Nanjing', 'Female'),
    ('Wudui', 'Male'),
    ('Midlands', 'Polygender')
]

start = time.time()

with db_session:
    for _ in range(100):
        for city, gender in city_gender:
            res = list(select(u for u in User if u.city == city and u.gender == gender))
            count += len(res)

now = time.time()
print(f"Pony ORM, select by gender and city: Rows/sec: {num_user_rows / (now - start): .2f}")

# Experiment 3a. Update whole
start = time.time()
count = 0
with db_session:
    count = len(list(select(u for u in User)))
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
