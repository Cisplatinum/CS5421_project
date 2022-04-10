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

# insert one
for i in range(num_rows):
    wh = warehouses[i]
    session.add(Bigtable(id=i+1,
                         first_name=wh['first_name'],
                         last_name=wh['last_name'],
                         email=wh['email'],
                         gender=wh['gender'],
                         ip_address=wh['ip_address'],
                         hometown_city=wh['hometown_city'],
                         contact_number=wh['contact_number'],
                         ssn=wh['ssn'],
                         credit_card=wh['credit_card'],
                         credit_card_type=wh['credit_card_type'],
                         job_title=wh['job_title'],
                         university=wh['university'],
                         Linkedin_skills=wh['Linkedin_skills']
                         ))
    session.commit()
now = time.time()

print(f"SQLAlchemy ORM, insert one bigtable: Rows/sec: {num_rows / (now - start): 10.2f}")
