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
        self.__url = None
        self.__engine = None
        self.__conn = None
        self.__host = host
        self.__username = username
        self.__port = port
        self.__db_name = db_name
        if password is not None:
            self.__password = quote_plus(password)
        else:
            self.__password = None
        self.__db_type = db_type
        self.__chunksize = 10000
        self.__operator = None
        self.__keep_conn = 0
        self.__package = None
        self.__charset = 'utf8mb4'
        self.__db_conn = {
            'host': str(host),
            'username': str(username),
            'password': '' if password is None else quote_plus(password),
            'port': str(port),
            'db_name': str(db_name)
        }

    def set_db(self, db_name):
        self.__db_name = db_name

    def set_chunksize(self, chunksize):
        self.__chunksize = chunksize

    def set_charset(self, charset):
        self.__charset = charset

    def set_package(self, package_name):
        self.__package = package_name

    def set_url(self, url):
        self.__url = url

    def __get_conn(self, load_file=False):
        if load_file:
            load_file = '&local_infile=1'
        else:
            load_file = ''
        base_url = '://{username}:{password}@{host}:{port}/{db_name}'.format(
            username=str(self.__username),
            password=str(self.__password),
            host=str(self.__host),
            port=str(self.__port),
            db_name=str(self.__db_name)
        )
        try:
            if self.__url is not None:
                url = self.__url
                engine = create_engine(url, poolclass=NullPool)
            elif self.__db_type.lower() == 'mysql':
                url = 'mysql+pymysql' + base_url + '?charset=' + str(self.__charset) + load_file
                engine = create_engine(url, poolclass=NullPool)
            elif self.__db_type.lower() == 'pgsql' or self.__db_type.lower() == 'postgresql':
                url = 'postgresql+psycopg2' + base_url
                engine = create_engine(url, poolclass=NullPool)
            elif self.__db_type.lower() == 'mssql':
                if str(self.__package) == 'pyodbc':
                    url = 'mssql+pyodbc' + base_url + '?driver=SQL+Server'
                    engine = create_engine(url, poolclass=NullPool, fast_executemany=True)
                else:
                    url = 'mssql+pymssql' + base_url
                    engine = create_engine(url, poolclass=NullPool)
            elif self.__db_type.lower() == 'oracle':
                url = 'oracle+cx_oracle' + base_url
                engine = create_engine(url, poolclass=NullPool, fast_executemany=True)
            elif self.__db_type.lower() == 'clickhouse':
                url = 'clickhouse+native' + base_url
                engine = create_engine(url, poolclass=NullPool, fast_executemany=True)
            elif self.__db_type.lower() == 'clickhouse-http':
                url = 'clickhouse:' + base_url
                engine = create_engine(url, poolclass=NullPool, fast_executemany=True)
            elif self.__db_type.lower() == 'sqlite':
                engine = create_engine('sqlite:///%s' % self.__host)
            else:
                raise Exception('不支持的数据库类型')
            conn = engine.connect()
        except Exception as e:
            raise Exception('连接出错: ' + str(e))
        return conn, engine

    def keep_conn(self):
        self.__keep_conn = 1
        self.__conn, self.__engine = self.__get_conn()

    def close_conn(self):
        try:
            self.__conn.close()
            self.__engine.dispose()
        except:
            pass

    @staticmethod
    def get_tmp_file():
        import tempfile
        _, file_path = tempfile.mkstemp()
        return file_path

    def get_sql(self, sql):
        if self.__keep_conn == 1:
            conn, engine = self.__conn, self.__engine
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
        if fast_load and str(self.__db_type).lower() == 'mysql':
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
                conn, engine = self.__conn, self.__engine
            else:
                conn, engine = self.__get_conn()
            df.to_sql(name=tb_name, con=conn, if_exists=how, index=False, chunksize=self.__chunksize)
            if self.__keep_conn == 0:
                conn.close()
                engine.dispose()

    def exe_sql(self, sql):
        if self.__keep_conn == 1:
            conn, engine = self.__conn, self.__engine
        else:
            conn, engine = self.__get_conn()

        if self.__db_type.lower() == 'mysql':
            from pymysql.constants import CLIENT
            import pymysql
            conn_pymysql = pymysql.Connection(
                host=self.__host,
                port=int(self.__port),
                user=self.__username,
                password=self.__password,
                database=self.__db_name,
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
        v = self.get_sql(sql).values[0][0]
        return v
