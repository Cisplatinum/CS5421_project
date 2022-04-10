import time
from model_big import Bigtable, engine
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)

session = Session()

# update the whole object
objs = list(session.query(Bigtable).all())
count = len(objs)
start = now = time.time()

for obj in objs:
    obj.hometown_city = f"{obj.hometown_city} Update2"
    session.add(obj)
session.commit()

now = time.time()

print(f"SQLAlchemy ORM, update one bigtable: Rows/sec: {count / (now - start): 10.2f}")
