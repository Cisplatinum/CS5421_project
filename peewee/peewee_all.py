from peewee import *
import config
import json
import time


psql_db = PostgresqlDatabase(
    config.DB['db'],
    host=config.DB['host'],
    port=config.DB['port'],
    user=config.DB['user'],
    password=config.DB['password']
)

# Define the objects stored in Warehouse
class Warehouse(Model):
    w_id = PrimaryKeyField(null=False)
    w_name = CharField(max_length=50)
    w_street = CharField(max_length=50)
    w_city = CharField(max_length=50)
    w_country = CharField(max_length=50)

    class Meta:
        database = psql_db
        db_table = 'warehouse'

        
# Define the objects stored in User
class User(Model):
    u_id = IntegerField(null=False)
    firstname = TextField()
    lastname = TextField()
    email = TextField()
    gender = TextField()
    ip = TextField()
    hometown_city = TextField()
    contact_number = TextField()
    ssn = TextField()
    credit_card = TextField()
    credit_card_type= TextField()
    job_title = TextField()
    university = TextField()
    linkedin_skills = TextField()

    class Meta:
        database = psql_db
        db_table = 'users'

# Ensure that the relations are created if they do not yet exist
psql_db.create_tables([Warehouse], safe=True)
psql_db.create_tables([User], safe=True)

print('---------- Small Table without Relations----------')

with open('warehouse.json', 'r') as f:
    warehouses = json.loads(f.read())

num_rows = len(warehouses)

# Experiment 1a: Single insert
start = time.time()

for i in range(num_rows):
    wh = warehouses[i]
    Warehouse(w_id=wh['w_id'],
        w_name=wh['w_name'],
        w_street=wh['w_street'],
        w_city=wh['w_city'],
        w_country=wh['w_country']
    ).save(force_insert=True)

end = time.time()
print(f'Peewee SINGLE INSERT Rows/sec: {num_rows/(end-start): 10.2f}')

# delete to conduct experiment on bulk insert
objs = list(Warehouse.select())
for obj in objs:
    obj.delete_instance()

# Experiment 1b: Batch insert (BATCH=50)
start = time.time()

BATCH_SIZE = 50
for i in range(0, num_rows, BATCH_SIZE):
    Warehouse.insert_many(
        [(warehouses[j]['w_id'], warehouses[j]['w_name'],
            warehouses[j]['w_street'], warehouses[j]['w_city'],
            warehouses[j]['w_country']) for j in range(i, i+BATCH_SIZE)],
        [Warehouse.w_id, Warehouse.w_name, Warehouse.w_street, Warehouse.w_city, Warehouse.w_country]
    ).execute()

end = time.time()
print(f'Peewee BATCH ({BATCH_SIZE}) INSERT Rows/sec: {num_rows/(end-start): .2f}')

# Experiment 2a: Single select by city
count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']
start = time.time()

for _ in range(100):
    for city in cities:
        res = list(Warehouse.select().where(Warehouse.w_city == city))
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT by city Rows/sec: {count/(end-start): .2f}')

# Experiment 2b: Single select by city and street
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
    for city, street in addresses:
        res = list(Warehouse.select().where(Warehouse.w_city == city, Warehouse.w_street == street))
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT by city and street Rows/sec: {count/(end-start): .2f}')

# Experiment 2c: Small
count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']
start = time.time()

for _ in range(100):
    for city in cities:
        res = list(Warehouse.select().where(Warehouse.w_city == city).limit(20))
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT by city limit 20 Rows/sec: {count/(end-start): .2f}')

# Experiment 2d: Dicts
count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']
start = time.time()

for _ in range(100):
    for city in cities:
        res = list(Warehouse.select().where(Warehouse.w_city == city).dicts())
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT and transform to dicts Rows/sec: {count/(end-start): .2f}')

# Experiment 2e: Tuples
count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']
start = time.time()

for _ in range(100):
    for city in cities:
        res = list(Warehouse.select().where(Warehouse.w_city == city).tuples())
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT and transform to tuples Rows/sec: {count/(end-start): .2f}')

# Experiment 3a. Update whole 

objs = list(Warehouse.select())
count = len(objs)
start = time.time()

for obj in objs:
    obj. w_id += 1
    obj. w_name= f"{obj.w_name} Update"
    obj. w_street= f"{obj.w_street} Update"
    obj. w_city= f"{obj.w_city} Update"
    obj. w_country= f"{obj.w_country} Update"
    obj.save()

end = time.time()
print(f"Peewee Update whole table Rows/sec: {count / (end - start): .2f}")


# Experiment 3b. Update partial (update only a single field w_country)
objs = list(Warehouse.select())
count = len(objs)
start = time.time()

for obj in objs:
    obj. w_country= f"{obj.w_country} Update2" 
    obj.save(only=["w_country"])

end = time.time()
print(f"Peewee Update partial Rows/sec: {count / (end - start): .2f}")


# Experiment 4. Delete
objs = list(Warehouse.select())
count = len(objs)
start = time.time()

for obj in objs:
    obj.delete_instance()

end = time.time()
print(f"Peewee Delete Rows/sec: {count / (end - start): .2f}")


print('---------- Small Table with Relations----------')

# Define the objects stored in Items
class Item(Model):
    i_id = PrimaryKeyField(null=False)
    i_im_id = FixedCharField(max_length=8)
    i_name = CharField(max_length=50)
    i_price = DecimalField(decimal_places=5, constraints=[Check('i_price > 0')])

    class Meta:
        database = psql_db
        db_table = 'items'

# Define the objects stored in Stock
class Stock(Model):
    w_id = ForeignKeyField(Warehouse, to_field='w_id')
    i_id = ForeignKeyField(Item, to_field='i_id')
    s_qty = SmallIntegerField(constraints=[Check('s_qty > 0')])

    class Meta:
        database = psql_db
        db_table = 'stocks'
        primary_key = CompositeKey('w_id', 'i_id')

# Ensure that the relations are created if they do not yet exist
psql_db.create_tables([Warehouse, Item, Stock], safe=True)

# Experiment 1a: Single insert
start = time.time()
for i in range(num_rows):
    wh = warehouses[i]
    Warehouse(w_id=wh['w_id'],
        w_name=wh['w_name'],
        w_street=wh['w_street'],
        w_city=wh['w_city'],
        w_country=wh['w_country']
    ).save(force_insert=True)
end = time.time()
print(f'Peewee SINGLE INSERT Rows/sec: {num_rows/(end-start): 10.2f}')

# delete to conduct experiment on bulk insert
objs = list(Warehouse.select())
for obj in objs:
    obj.delete_instance()

# Experiment 1b: Batch insert (BATCH=50)
start = time.time()

BATCH_SIZE = 50
for i in range(0, num_rows, BATCH_SIZE):
    Warehouse.insert_many(
        [(warehouses[j]['w_id'], warehouses[j]['w_name'],
            warehouses[j]['w_street'], warehouses[j]['w_city'],
            warehouses[j]['w_country']) for j in range(i, i+BATCH_SIZE)],
        [Warehouse.w_id, Warehouse.w_name, Warehouse.w_street, Warehouse.w_city, Warehouse.w_country]
    ).execute()

end = time.time()
print(f'Peewee BATCH ({BATCH_SIZE}) INSERT Rows/sec: {num_rows/(end-start): .2f}')

# Experiment 2a: Single select by city
count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']

start = time.time()

for _ in range(100):
    for city in cities:
        res = list(Warehouse.select().where(Warehouse.w_city == city))
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT by city Rows/sec: {count/(end-start): .2f}')

# Experiment 2b: Single select by city and street
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
    for city, street in addresses:
        res = list(Warehouse.select().where(Warehouse.w_city == city, Warehouse.w_street == street))
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT by city and street Rows/sec: {count/(end-start): .2f}')

# Experiment 2c: Small
count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']

start = time.time()

for _ in range(100):
    for city in cities:
        res = list(Warehouse.select().where(Warehouse.w_city == city).limit(20))
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT by city limit 20 Rows/sec: {count/(end-start): .2f}')

# Experiment 2d: Dicts
count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']

start = time.time()

for _ in range(100):
    for city in cities:
        res = list(Warehouse.select().where(Warehouse.w_city == city).dicts())
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT and transform to dicts Rows/sec: {count/(end-start): .2f}')

# Experiment 2e: Tuples
count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']

start = time.time()

for _ in range(100):
    for city in cities:
        res = list(Warehouse.select().where(Warehouse.w_city == city).tuples())
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT and transform to tuples Rows/sec: {count/(end-start): .2f}')

# Experiment 3a. Update whole 

objs = list(Warehouse.select())
count = len(objs)

start = time.time()

for obj in objs:
    obj. w_id += 1
    obj. w_name= f"{obj.w_name} Update"
    obj. w_street= f"{obj.w_street} Update"
    obj. w_city= f"{obj.w_city} Update"
    obj. w_country= f"{obj.w_country} Update"
    obj.save()

end = time.time()

print(f"Peewee Update whole table Rows/sec: {count / (end - start): .2f}")


# Experiment 3b. Update partial (update only a single field w_country)
objs = list(Warehouse.select())
count = len(objs)

start = time.time()

for obj in objs:
    obj. w_country= f"{obj.w_country} Update2" 
    obj.save(only=["w_country"])

end = time.time()

print(f"Peewee Update partial Rows/sec: {count / (end - start): .2f}")


# Experiment 4. Delete
objs = list(Warehouse.select())
count = len(objs)

start = time.time()

for obj in objs:
    obj.delete_instance()

end = time.time()

print(f"Peewee Delete Rows/sec: {count / (end - start): .2f}")

# To run the JOIN experiments
# # Experiment 5a: Single select stocks join warehouse by city
# count = 0
# cities = [
#     'Santa Cruz', 
#     'Washington', 
#     'Bibrka', 
#     'Haninge', 
#     'Inta'
# ]

# start = time.time()

# for _ in range(100):
#     for city in cities:
#         res = list(Stock.select().join(Warehouse)
#             .where(Warehouse.w_city == city))
#         count += len(res)

# end = time.time()
# print(f'Peewee Single SELECT stock join warehouse by city Rows/sec: {count/(end-start): .2f}')

# # Experiment 5b: Single select stocks join warehouse by city and item.name
# count = 0
# city_iname = [
#     ('Stockholm', 'Pedi-Dri'), 
#     ('Hesheng', 'Glyburide'), 
#     ('Ciekek', 'Hydrocortisone'), 
#     ('Oslo', 'ACNE SOLUTIONS'), 
#     ('Washington', 'Doxycycline Hyclate')
# ]

# start = time.time()

# for _ in range(100):
#     for city, i_name in city_iname:
#         res = list(Item.select().join(Stock).join(Warehouse)
#             .where(Warehouse.w_city == city, Item.i_name == i_name))
#         count += len(res)

# end = time.time()
# print(f'Peewee Single SELECT stock join warehouse by city and i_name Rows/sec: {count/(end-start): .2f}')


print('---------- Big Table without Relations----------')
with open('bigtable.json', 'r') as f:
    users = json.loads(f.read())

num_rows = len(users)

# Experiment 1a: Single insert
start = time.time()

for i in range(num_rows):
    u = users[i]
    User(u_id = u['id'],
        firstname = u['first_name'],
        lastname = u['last_name'],
        email = u['email'],
        gender = u['gender'],
        ip = u['ip_address'],
        hometown_city = u['hometown_city'],
        contact_number = u['contact_number'],
        ssn = u['ssn'],
        credit_card = u['credit_card'],
        credit_card_type = u['credit_card_type'],
        job_title = u['job_title'],
        university = u['university'],
        linkedin_skills = u['Linkedin_skills']
    ).save(force_insert=True)

end = time.time()
print(f'Peewee SINGLE INSERT Rows/sec: {num_rows/(end-start): 10.2f}')
# delete to conduct experiment on bulk insert
objs = list(User.select())
for obj in objs:
    obj.delete_instance()

# Experiment 1b: Batch insert (BATCH=50)
start = time.time()

BATCH_SIZE = 50
for i in range(0, num_rows, BATCH_SIZE):
    User.insert_many(
        [(users[j]['id'],users[j]['first_name'],users[j]['last_name'],
            users[j]['email'],users[j]['gender'],users[j]['ip_address'],
            users[j]['hometown_city'],users[j]['contact_number'], users[j]['ssn'],
            users[j]['credit_card'],users[j]['credit_card_type'], users[j]['job_title'],
            users[j]['university'], users[j]['Linkedin_skills']) for j in range(i, i+BATCH_SIZE)],
        [User.u_id, User.firstname, User.lastname, User.email, User.gender, User.ip,
        User.hometown_city, User.contact_number, User.ssn, User.credit_card,
        User.credit_card_type, User.job_title, User.university, User.linkedin_skills]
    ).execute()

end = time.time()
print(f'Peewee BATCH ({BATCH_SIZE}) INSERT Rows/sec: {num_rows/(end-start): .2f}')

# Experiment 2a: Single select by city
count = 0
cities = ['Nanjing','Wudui','Midlands']
start = time.time()

for _ in range(100):
    for city in cities:
        res = list(User.select().where(User.hometown_city == city))
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT by city Rows/sec: {count/(end-start): .2f}')

# Experiment 2b: Single select by gender and city
count = 0
city_gender = [
    ('Nanjing', 'Female'),
    ('Wudui', 'Male'),
    ('Midlands', 'Polygender')
]

start = time.time()

for _ in range(100):
    for city, gender in city_gender:
        res = list(User.select().where(User.gender == gender, User.hometown_city == city))
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT by city and gender Rows/sec: {count/(end-start): .2f}')

# Experiment 2c: Small
count = 0
cities = ['Nanjing','Wudui','Midlands']
start = time.time()

for _ in range(100):
    for city in cities:
        res = list(User.select().where(User.hometown_city == city).limit(20))
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT by city limit 20 Rows/sec: {count/(end-start): .2f}')
# Experiment 2d: Dicts
count = 0
genders = ['Female','Polygender']
start = time.time()

for _ in range(100):
    for gender in genders:
        res = list(User.select().where(User.gender == gender).dicts())
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT and transform to dicts Rows/sec: {count/(end-start): .2f}')

# Experiment 2e: Tuples
count = 0
genders = ['Female','Polygender']
start = time.time()

for _ in range(100):
    for gender in genders:
        res = list(User.select().where(User.gender == gender).tuples())
        count += len(res)

end = time.time()
print(f'Peewee Single SELECT and transform to tuples Rows/sec: {count/(end-start): .2f}')


# Experiment 3a. Update whole 

objs = list(User.select())
count = len(objs)

start = time.time()

for obj in objs:
    obj.u_id += 1
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
    obj.save()

end = time.time()

print(f"Peewee Update whole table Rows/sec: {count / (end - start): .2f}")


# Experiment 3b. Update partial (update only a single field city)
objs = list(User.select())
count = len(objs)

start = time.time()

for obj in objs:
    obj.hometown_city= f"{obj.hometown_city} Update2" 
    obj.save(only=["hometown_city"])

end = time.time()

print(f"Peewee Update partial Rows/sec: {count / (end - start): .2f}")


# Experiment 4. Delete
objs = list(User.select())
count = len(objs)

start = time.time()

for obj in objs:
    obj.delete_instance()

end = time.time()

print(f"Peewee Delete Rows/sec: {count / (end - start): .2f}")
