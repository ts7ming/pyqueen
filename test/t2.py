import importlib.util
import sys,os
import importlib

sys.path.append("..")
from pyqueen import DataSource

from sqlalchemy import create_engine, Column, Integer, String, Boolean, BigInteger,DateTime,DECIMAL
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Tb1(Base):  # 表结构
    __tablename__ = 'tb_fact'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(100), nullable=False)
    create_time = Column(DateTime)
    org_id = Column(BigInteger)
    user_id = Column(Integer)
    is_delete = Column(Boolean, default=False)
    amt = Column(DECIMAL(10,2))



ds1 = DataSource(conn_type='sqlite',file_path='test.db')
ddl1 = ds1.generate_ddl(Base)

ds2 = DataSource(conn_type='mysql',host='localhost',username='root',password='qiming',port='3306',db_name='cdc_source')
ddl2 = ds2.generate_ddl(Base)

ds3 = DataSource(conn_type='mssql',host='localhost',username='sa',password='1qaz2wsx.A',port='1433',db_name='finance')
ddl3 = ds3.generate_ddl(Base)

print(ds1.conn_type+'         方言')
for d in ddl1:
    print(d)

print(ds2.conn_type+'         方言')
for d in ddl2:
    print(d)

print(ds3.conn_type+'         方言')
for d in ddl3:
    print(d)