

from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker

from sqlalchemy import create_engine


class DynamicRepo:
    def __init__(self):
        self.__ds = None
        self.default_list = None
        self.table_name = self.__dict__['_table_name']
        self.table_comment = self.__dict__['_table_comment']
        self.columns = [col for col in self.__dict__.values() if type(col) is 'Field']
        self.__sqlalchemy_model()

    @contextmanager
    def session_context(self):
        engine = create_engine(self.__ds.get_jdbc_url(), echo=True)
        session_obj = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = session_obj()
        try:
            yield session
        except Exception as e:
            raise Exception(e)
        finally:
            session.close()