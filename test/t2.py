from ..pyqueen.io.data_source import DataSource

ds = DataSource(conn_type='sqlite',file_path='test.db')

from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()
class Tb1(Base):
    __tablename__ = 'articles'
    id = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    content = Column(String(1000))
    user_id = Column(Integer, ForeignKey('users.id'))

# ddl = ds.generate_ddl(Base)
ddl = ds.g()

print(ddl)