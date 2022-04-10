from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

engine = create_engine('postgresql://yixingw:wyx971002@localhost:5432/project')
Base = declarative_base()


class Warehouse(Base):
    __tablename__ = "warehouse"
    w_id = Column(Integer, primary_key=True)
    w_name = Column(String(64), nullable=False)
    w_street = Column(String(64), nullable=False)
    w_city = Column(String(64), nullable=False)
    w_country = Column(String(64), nullable=False)
    stocks = relationship("Stock", back_populates="warehouses")


class Item(Base):
    __tablename__ = "items"
    i_id = Column(Integer, primary_key=True)
    i_im_id = Column(String(8), nullable=False)
    i_name = Column(String(64), nullable=False)
    i_price = Column(Float(precision=5), nullable=False)
    stocks = relationship("Stock", back_populates="items")


class Stock(Base):
    __tablename__ = "stocks"
    w_id = Column(Integer, ForeignKey("warehouse.w_id"), primary_key=True)
    i_id = Column(Integer, ForeignKey("items.i_id"), primary_key=True)
    s_qty = Column(Integer, nullable=False)
    warehouses = relationship("Warehouse", back_populates="stocks")
    items = relationship("Item", back_populates="stocks")


Base.metadata.create_all(engine)
