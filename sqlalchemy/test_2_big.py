import json
import time

from sqlalchemy.orm import sessionmaker

from model_big import Bigtable, engine

with open('bigtable.json', 'r') as f:
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
            Bigtable(id=j + 1,
                     first_name=warehouses[j]['first_name'],
                     last_name=warehouses[j]['last_name'],
                     email=warehouses[j]['email'],
                     gender=warehouses[j]['gender'],
                     ip_address=warehouses[j]['ip_address'],
                     hometown_city=warehouses[j]['hometown_city'],
                     contact_number=warehouses[j]['contact_number'],
                     ssn=warehouses[j]['ssn'],
                     credit_card=warehouses[j]['credit_card'],
                     credit_card_type=warehouses[j]['credit_card_type'],
                     job_title=warehouses[j]['job_title'],
                     university=warehouses[j]['university'],
                     Linkedin_skills=warehouses[j]['Linkedin_skills']
                     ) for j in range(i, i + batch_size)
        ]
    )
    session.commit()
now = time.time()

print(f"SQLAlchemy ORM, insert by batch bigtable: Rows/sec: {num_rows / (now - start): 10.2f}")
