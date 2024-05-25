import os
import sys
from sqlalchemy import create_engine, text
import pandas as pd


class SqlDB:
    def __init__(self, conn_package=None, db_type=None, host=None, username=None, password=None, port=None, db_name=None, jdbc_url=None,
                 keep_conn=False):
        self.__conn = None
        self.__engine = None
        self.__base_param = {
            'db_type': db_type,
            'package': str(conn_package),
            'username': str(username),
            'password': str(password),
            'host': str(host),
            'port': str(port),
            'db_name': str(db_name),
        }
        self.__url = jdbc_url
        self.__ext_param = None
        self.__keep_conn = keep_conn

    def set_ext_param(self, **kwargs):
        self.__url = None
        self.__ext_param = {}
        for k, v in kwargs.items():
            self.__ext_param[k] = v

    def create_conn(self):
        if self.__keep_conn is False or self.__conn is None:
            if self.__url is None:
                self.__url = '{db_type}+{package}://{username}:{password}@{host}:{port}/{db_name}'
                self.__url = self.__url.format(**self.__base_param)
                not_first_param = False
                if self.__ext_param is not None and len(self.__ext_param.keys()) > 0:
                    self.__url = self.__url + '?'
                    for pk, pv in self.__ext_param.items():
                        if not_first_param:
                            pk = '&' + pk
                        self.__url = self.__url + pk + '=' + pv
                        not_first_param = True
            self.__engine = create_engine(self.__url)
            self.__conn = self.__engine.connect()

    def close_conn(self):
        if self.__keep_conn is False:
            try:
                self.__conn.close()
                self.__engine.dispose()
            except:
                pass

    def read_sql(self, sql):
        self.create_conn()
        try:
            sql = text(sql)
            df = pd.read_sql(sql, self.__conn)
        except Exception as e:
            raise Exception(str(e)[0:500])
        finally:
            self.close_conn()
        return df

    def to_db(self, df, tb_name, how, chunksize, fast_load=False):
        self.create_conn()
        try:
            df.to_sql(name=tb_name, con=self.__conn, if_exists=how, index=False, chunksize=chunksize)
        except Exception as e:
            raise Exception(str(e)[0:500])
        finally:
            self.close_conn()

    def exe_sql(self, sql):
        self.create_conn()
        if isinstance(sql, list):
            try:
                for sql_text in sql:
                    sql_text = text(sql_text)
                    self.__conn.execute(sql_text)
                    self.__conn.commit()
            except Exception as e:
                raise Exception('执行sql出错: ' + str(e)[0:500])
            finally:
                self.close_conn()
        else:
            sql = text(sql)
            try:
                self.__conn.execute(sql)
                self.__conn.commit()
            except Exception as e:
                raise Exception('执行sql出错: ' + str(e)[0:500])
            finally:
                self.close_conn()

    @staticmethod
    def get_tmp_file():
        import tempfile
        _, file_path = tempfile.mkstemp()
        file_path = str(file_path).replace('\\', '\\\\')
        return file_path

    def set_db(self, db_name):
        self.__base_param['db_name'] = db_name


class MySQL(SqlDB):
    def __init__(self, host, username, password, port, db_name, charset='utf8mb4', conn_package='pymysql', keep_conn=False, jdbc_url=None):
        super().__init__(
            conn_package=conn_package,
            db_type='mysql',
            host=host,
            username=username,
            password=password,
            port=port,
            db_name=db_name,
            jdbc_url=jdbc_url,
            keep_conn=keep_conn
        )
        self.__charset = charset
        super().set_ext_param(charset=self.__charset)

    def to_db(self, df, tb_name, how, chunksize, fast_load):
        if fast_load:
            super().set_ext_param(charset=self.__charset, local_infile='1')
            file = self.get_tmp_file()
            df.to_csv(file, index=False, quoting=1)
            sql = '''
                LOAD DATA LOCAL INFILE '%s' INTO TABLE %s Fields Terminated By ',' Enclosed By '"' IGNORE 1 LINES;
            ''' % (file, tb_name)
            self.exe_sql(sql)
            try:
                os.unlink(file)
            except Exception as e:
                print('failed to delete file: ' + file + '   ' + str(e)[0:100])
        else:
            super().to_db(df=df, tb_name=tb_name, how=how, chunksize=chunksize)


class PostgresSQL(SqlDB):
    def __init__(self, host, username, password, port, db_name, conn_package='psycopg2', keep_conn=False, jdbc_url=None):
        super().__init__(
            conn_package=conn_package,
            db_type='postgresql',
            host=host,
            username=username,
            password=password,
            port=port,
            db_name=db_name,
            jdbc_url=jdbc_url,
            keep_conn=keep_conn
        )


class MSSQL(SqlDB):
    def __init__(self, host, username, password, port, db_name, conn_package='pymssql', keep_conn=False, jdbc_url=None):
        super().__init__(
            conn_package=conn_package,
            db_type='mssql',
            host=host,
            username=username,
            password=password,
            port=port,
            db_name=db_name,
            jdbc_url=jdbc_url,
            keep_conn=keep_conn
        )


class Oracle(SqlDB):
    def __init__(self, host, username, password, port, db_name, conn_package='cx_oracle', keep_conn=False, jdbc_url=None):
        super().__init__(
            conn_package=conn_package,
            db_type='oracle',
            host=host,
            username=username,
            password=password,
            port=port,
            db_name=db_name,
            jdbc_url=jdbc_url,
            keep_conn=keep_conn
        )


class Cliskhouse(SqlDB):
    def __init__(self, host, username, password, port, db_name, conn_package='native', keep_conn=False, jdbc_url=None):
        super().__init__(
            conn_package=conn_package,
            db_type='clickhouse',
            host=host,
            username=username,
            password=password,
            port=port,
            db_name=db_name,
            jdbc_url=jdbc_url,
            keep_conn=keep_conn
        )
        self.__host = host
        self.__username = username
        self.__password = password
        self.__port = port
        self.__db_name = db_name

    def to_db(self, df, tb_name, how, chunksize, fast_load):
        if fast_load:
            import csv
            file_path = self.get_tmp_file()
            df.to_csv(file_path, index=False, sep='\t', quoting=csv.QUOTE_NONE, escapechar='\t', na_rep='\\N')
            query = 'INSERT INTO {db_name}.{table_name} FORMAT TabSeparatedWithNames'
            query = query.format(db_name=self.__db_name, table_name=tb_name)
            cmd = 'cat {file_path} | clickhouse-client --host {host} --port {port} --user {user} --password {password} --query "{query}"'
            cmd = cmd.format(file_path=file_path, host=self.__host, port=str(self.__port), user=self.__username, password=self.__password,
                             query=query)
            os.system(cmd)
            try:
                os.unlink(file_path)
            except:
                pass
        else:
            super().to_db(df=df, tb_name=tb_name, how=how, chunksize=chunksize)


class Sqlite(SqlDB):
    def __init__(self, file_path=None, jdbc_url=None):
        if jdbc_url is None:
            jdbc_url = 'sqlite:///%s' % str(file_path)
        super().__init__(
            jdbc_url=jdbc_url
        )
