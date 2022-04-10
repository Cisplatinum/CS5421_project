import json
import time

from model_big import Bigtable, engine
from sqlalchemy.orm import sessionmaker

with open('bigtable.json', 'r') as f:
    warehouses = json.loads(f.read())

num_rows = len(warehouses)

Session = sessionmaker(bind=engine)
start = now = time.time()
session = Session()

# insert by bulk
session.bulk_save_objects([Bigtable(id=i + 1,
                                    first_name=warehouses[i]['first_name'],
                                    last_name=warehouses[i]['last_name'],
                                    email=warehouses[i]['email'],
                                    gender=warehouses[i]['gender'],
                                    ip_address=warehouses[i]['ip_address'],
                                    hometown_city=warehouses[i]['hometown_city'],
                                    contact_number=warehouses[i]['contact_number'],
                                    ssn=warehouses[i]['ssn'],
                                    credit_card=warehouses[i]['credit_card'],
                                    credit_card_type=warehouses[i]['credit_card_type'],
                                    job_title=warehouses[i]['job_title'],
                                    university=warehouses[i]['university'],
                                    Linkedin_skills=warehouses[i]['Linkedin_skills']
                                    ) for i in range(num_rows)])
session.commit()
now = time.time()

print(f"SQLAlchemy ORM, insert by bulk bigtable: Rows/sec: {num_rows / (now - start): 10.2f}")
