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
    session.delete(obj)
session.commit()

now = time.time()

print(f"SQLAlchemy ORM, delete bigtable: Rows/sec: {count / (now - start): 10.2f}")
