from sqlalchemy import (
    Column,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('postgresql://yixingw:wyx971002@localhost:5432/project')
Base = declarative_base()


class Bigtable(Base):
    __tablename__ = "bigtable"
    id = Column(Integer, primary_key=True)
    first_name = Column(String(64), nullable=False)
    last_name = Column(String(64), nullable=False)
    email = Column(String(64), nullable=False)
    gender = Column(String(64), nullable=False)
    ip_address = Column(String(64), nullable=False)
    hometown_city = Column(String(64), nullable=False)
    contact_number = Column(String(64), nullable=False)
    ssn = Column(String(64), nullable=False)
    credit_card = Column(String(64), nullable=False)
    credit_card_type = Column(String(64), nullable=False)
    job_title = Column(String(64), nullable=False)
    university = Column(String(256), nullable=False)
    Linkedin_skills = Column(String(64), nullable=False)


Base.metadata.create_all(engine)
