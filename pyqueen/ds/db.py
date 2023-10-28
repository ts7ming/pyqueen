import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import os
from urllib.parse import quote_plus

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class DB(object):
    """
    数据源管理
    """

    def __init__(self, host=None, username=None, password=None, port=None, db_name=None, db_type='MySQL'):
        self.host = host
        self.db_type = db_type
        self.chunksize = 10000
        self.__operator = None
        self.__keep_conn = 0
        self.charset = 'utf8mb4'
        if db_type.lower() != 'sqlite':
            self.username = username
            self.password = quote_plus(password)
            self.port = port
            self.db_name = db_name
        self.__db_conn = {
            'host': str(host),
            'username': str(username),
            'password': '' if password is None else quote_plus(password),
            'port': str(port),
            'db_name': str(db_name)
        }

    def set_db(self, db_name):
        self.db_name = db_name

    def set_chunksize(self, chunksize):
        self.chunksize = chunksize

    def set_charset(self, charset):
        self.charset = charset

    def __get_conn(self, load_file=False):
        if load_file:
            param = '&local_infile=1'
        else:
            param = ''
        try:
            if self.db_type.lower() == 'mysql':
                engine = create_engine(
                    "mysql+pymysql://{}:{}@{}:{}/{}?charset={}{}".format(
                        self.username,
                        self.password,
                        str(self.host),
                        str(self.port),
                        str(self.db_name),
                        self.charset,
                        param,
                    ),
                    poolclass=NullPool
                )
            elif self.db_type.lower() == 'mssql':
                engine = create_engine(
                    "mssql+pyodbc://{}:{}@{}:{}/{}?driver=SQL+Server".format(
                        self.username,
                        self.password,
                        str(self.host),
                        str(self.port),
                        str(self.db_name)
                    ),
                    poolclass=NullPool,
                    fast_executemany=True
                )
            elif self.db_type.lower() == 'oracle':
                import cx_Oracle
                engine = create_engine(
                    "oracle+cx_oracle://{}:{}@{}:{}/{}".format(
                        self.username,
                        self.password,
                        str(self.host),
                        str(self.port),
                        str(self.db_name)
                    ),
                    poolclass=NullPool
                )
            elif self.db_type.lower() == 'clickhouse':
                engine = create_engine(
                    "clickhouse+native://{}:{}@{}:{}/{}".format(
                        self.username,
                        self.password,
                        str(self.host),
                        str(self.port),
                        str(self.db_name)
                    ),
                    poolclass=NullPool
                )
            elif self.db_type.lower() == 'clickhouse-http':
                engine = create_engine(
                    "clickhouse://{}:{}@{}:{}/{}".format(
                        self.username,
                        self.password,
                        str(self.host),
                        str(self.port),
                        str(self.db_name)
                    ),
                    poolclass=NullPool
                )
            elif self.db_type.lower() == 'sqlite':
                engine = create_engine('sqlite:///%s' % self.host)
            else:
                raise Exception('不支持的数据库类型')
            conn = engine.connect()
        except Exception as e:
            raise Exception('连接出错: ' + str(e))
        return conn, engine

    def keep_conn(self):
        self.__keep_conn = 1
        self.conn, self.engine = self.__get_conn()

    def close_conn(self):
        try:
            self.conn.close()
            self.engine.dispose()
        except:
            pass

    @staticmethod
    def get_tmp_file():
        import tempfile
        _, file_path = tempfile.mkstemp()
        return file_path

    def get_sql(self, sql):
        if self.__keep_conn == 1:
            conn, engine = self.conn, self.engine
        else:
            conn, engine = self.__get_conn()
        try:
            df = pd.read_sql(sql, conn)
        except Exception as e:
            raise Exception('读取sql出错 ' + sql[0:50] + str(e)[0:500])
        finally:
            if self.__keep_conn == 0:
                conn.close()
                engine.dispose()
        return df

    def to_db(self, df, tb_name: str, fast_load: str = False, how: str = 'append'):
        if fast_load and str(self.db_type).lower() == 'mysql':
            file = self.get_tmp_file()
            df.to_csv(file, index=False, quoting=1)
            conn, engine = self.__get_conn(load_file=True)
            sql = '''
                LOAD DATA LOCAL INFILE '%s' INTO TABLE %s Fields Terminated By ',' Enclosed By '"' IGNORE 1 LINES;
            ''' % (file, tb_name)
            try:
                engine.execute(sql)
            except Exception as e:
                raise Exception('导入数据出错: ' + str(e)[0:500])
            finally:
                if self.__keep_conn == 0:
                    conn.close()
                    engine.dispose()
            try:
                os.unlink(file)
            except:
                pass
        else:
            if self.__keep_conn == 1:
                conn, engine = self.conn, self.engine
            else:
                conn, engine = self.__get_conn()
            df.to_sql(name=tb_name, con=conn, if_exists=how, index=False, chunksize=self.chunksize)
            if self.__keep_conn == 0:
                conn.close()
                engine.dispose()

    def exe_sql(self, sql):
        if self.__keep_conn == 1:
            conn, engine = self.conn, self.engine
        else:
            conn, engine = self.__get_conn()

        if self.db_type.lower() == 'mysql':
            from pymysql.constants import CLIENT
            import pymysql
            conn_pymysql = pymysql.Connection(
                host=self.host,
                port=int(self.port),
                user=self.username,
                password=self.password,
                database=self.db_name,
                charset='utf8',
                client_flag=CLIENT.MULTI_STATEMENTS
            )
            try:
                cursor = conn_pymysql.cursor()
                if isinstance(sql, list):
                    for sql_text in sql:
                        sql_text = sql_text.replace('%', '%%')
                        cursor.execute(sql_text)
                else:
                    sql = sql.replace('%', '%%')
                    cursor.execute(sql)
                conn_pymysql.commit()
                cursor.close()
            except Exception as e:
                raise Exception('执行sql出错: ' + str(e)[0:500])
            finally:
                conn_pymysql.close()

            if self.__keep_conn == 0:
                conn.close()
                engine.dispose()
        else:
            try:
                if isinstance(sql, list):
                    for sql_text in sql:
                        sql_text = sql_text.replace('%', '%%')
                        engine.execute(sql_text)
                else:
                    sql = sql.replace('%', '%%')
                    engine.execute(sql)
            except Exception as e:
                raise Exception('执行sql出错: ' + str(e)[0:500])
            finally:
                if self.__keep_conn == 0:
                    conn.close()
                    engine.dispose()

    def get_value(self, sql):
        df = self.get_sql(sql)
        rows = df.values[0][0]
        return int(rows)
