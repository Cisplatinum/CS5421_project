from sqlalchemy import (
    Column,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('postgresql://yixingw:wyx971002@localhost:5432/project')
Base = declarative_base()


class WarehouseS(Base):
    __tablename__ = "warehouseS"
    w_id = Column(Integer, primary_key=True)
    w_name = Column(String(64), nullable=False)
    w_street = Column(String(64), nullable=False)
    w_city = Column(String(64), nullable=False)
    w_country = Column(String(64), nullable=False)


Base.metadata.create_all(engine)
