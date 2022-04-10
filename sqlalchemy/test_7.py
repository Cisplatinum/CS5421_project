import time

from model import Warehouse, Stock, Item, engine
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)

session = Session()

# update the whole object
count = 0
city_iname = [
        ('Stockholm', 'Pedi-Dri'),
        ('Hesheng', 'Glyburide'),
        ('Ciekek', 'Hydrocortisone'),
        ('Oslo', 'ACNE SOLUTIONS'),
        ('Washington', 'Doxycycline Hyclate')
]


start = now = time.time()

for _ in range(100):
    for city, i_name in city_iname:
        res = list(session.query(Item).join(Stock).join(Warehouse)
                   .where(Warehouse.w_city == city, Item.i_name == i_name))
        count += len(res)

now = time.time()
print(count)
print(f"SQLAlchemy ORM, join filter by city and i_name: Rows/sec: {count / (now - start): 10.2f}")
