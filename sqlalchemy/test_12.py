import time

from model import Warehouse, engine
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)

session = Session()

# update the whole object
objs = list(session.query(Warehouse).all())
count = len(objs)
start = now = time.time()

for obj in objs:
    session.delete(obj)
session.commit()

now = time.time()

print(f"SQLAlchemy ORM, delete: Rows/sec: {count / (now - start): 10.2f}")
