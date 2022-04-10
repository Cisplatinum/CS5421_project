import time

from model import Warehouse, engine
from sqlalchemy.orm import sessionmaker

cities = ['Stockholm', 'Oslo', 'Buenavista', 'San Antonio', 'Washington']

Session = sessionmaker(bind=engine)

session = Session()

# update the whole object
objs = list(session.query(Warehouse).all())
count = len(objs)
start = now = time.time()

for obj in objs:
    obj.w_name = f"{obj.w_name} Update"
    obj.w_street = f"{obj.w_street} Update"
    obj.w_city = f"{obj.w_city} Update"
    obj.w_country = f"{obj.w_country} Update"
    session.add(obj)
session.commit()

now = time.time()

print(f"SQLAlchemy ORM, update all: Rows/sec: {count / (now - start): 10.2f}")
