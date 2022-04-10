import time
from model_big import Bigtable, engine
from sqlalchemy.orm import sessionmaker

count = 0
cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']

Session = sessionmaker(bind=engine)
session = Session()

# update the whole object
objs = list(session.query(Bigtable).all())

count = len(objs)
start = now = time.time()

for obj in objs:
    obj.first_name = f"{obj.first_name} Update",
    obj.last_name = f"{obj.last_name} Update",
    obj.email = f"{obj.email} Update",
    obj.gender = f"{obj.gender} Update",
    obj.ip_address = f"{obj.ip_address} Update",
    obj.hometown_city = f"{obj.hometown_city} Update",
    obj.contact_number = f"{obj.contact_number} Update",
    obj.ssn = f"{obj.ssn} Update",
    obj.credit_card = f"{obj.credit_card} Update",
    obj.credit_card_type = f"{obj.credit_card_type} Update",
    obj.job_title = f"{obj.job_title} Update",
    obj.university = f"{obj.university} Update",
    obj.Linkedin_skills = f"{obj.Linkedin_skills} Update",
    session.add(obj)
session.commit()

now = time.time()

print(f"SQLAlchemy ORM, update all bigtable: Rows/sec: {count / (now - start): 10.2f}")
