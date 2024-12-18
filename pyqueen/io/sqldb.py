import os
from sqlalchemy import create_engine, text
import pandas as pd
import inspect
from urllib.parse import quote_plus, urlencode


class SqlDB:
    def __init__(self,
                 conn_package=None,
                 db_type=None,
                 host=None,
                 username=None,
                 password=None,
                 port=None,
                 db_name=None,
                 jdbc_url=None,
                 keep_conn=False,
                 conn_params=None
                 ):
        password = password if password is not None else ''
        self.__conn = None
        self.__engine = None

        self.__base_param = {
            'db_type': db_type,
            'package': conn_package,
            'username': username,
            'password': quote_plus(password),
            'host': host,
            'port': port,
            'db_name': db_name,
        }

        self.__url = jdbc_url
        self.__ext_param = {}
        self.__conn_params = conn_params
        self.__keep_conn = keep_conn

    def set_ext_param(self, **kwargs):
        self.__url = None
        self.__ext_param = {}
        for k, v in kwargs.items():
            self.__ext_param[k] = v
        if self.__conn_params is not None:
            for k, v in self.__conn_params.items():
                self.__ext_param[k] = v

    def __build_url(self):
        if self.__url is None:
            base_url = f"{self.__base_param['db_type']}+{self.__base_param['package']}://{self.__base_param['username']}:{self.__base_param['password']}@{self.__base_param['host']}:{self.__base_param['port']}/{self.__base_param['db_name']}"
            if self.__ext_param:
                query_params = urlencode(self.__ext_param)
                self.__url = f"{base_url}?{query_params}"
            else:
                self.__url = base_url

    def create_conn(self):
        if not self.__keep_conn or self.__conn is None:
            self.__build_url()
            try:
                self.__engine = create_engine(self.__url)
                self.__conn = self.__engine.connect()
            except Exception as e:
                raise Exception(f"无法创建数据库连接: {e}")

    def close_conn(self, engine=None, conn=None):
        if self.__keep_conn is False:
            if engine is None:
                engine = self.__engine
                conn = self.__conn
            try:
                conn.close()
                engine.dispose()
            except Exception as e:
                print(e)

    def read_sql(self, sql, data=None, engine='sqlite'):
        if data is not None:
            from pyqueen.io.excel import Excel
            e = Excel()
            df = e.read_sql(sql=sql, data=data, engine=engine)
            return df
        else:
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

    def exe_sql(self, sql, auto_commit=False):
        def execute_statements(connection, statements):
            for sql_text in statements:
                sql_text = text(sql_text)
                connection.execute(sql_text)
                connection.commit()

        try:
            if auto_commit:
                self.__build_url()
                with create_engine(self.__url).connect() as conn:
                    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                    if isinstance(sql, list):
                        execute_statements(conn, sql)
                    else:
                        conn.execute(text(sql))
                        conn.commit()
            else:
                self.create_conn()
                try:
                    if isinstance(sql, list):
                        execute_statements(self.__conn, sql)
                    else:
                        self.__conn.execute(text(sql))
                        self.__conn.commit()
                finally:
                    if not self.__keep_conn:
                        self.close_conn()
        except Exception as e:
            raise Exception(f'执行sql出错: {e}')

    @staticmethod
    def get_tmp_file():
        import tempfile
        _, file_path = tempfile.mkstemp()
        file_path = str(file_path).replace('\\', '\\\\')
        return file_path

    @staticmethod
    def to_excel(sheet_list, file_path=None, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11, column_width=17):
        from pyqueen.io.excel import Excel
        e = Excel(file_path=file_path)
        return e.to_excel(sheet_list=sheet_list, fillna=fillna, fmt=fmt, font=font, font_color=font_color, font_size=font_size, column_width=column_width)

    @staticmethod
    def read_excel(sheet_name=None, file_path=None):
        from pyqueen.io.excel import Excel
        e = Excel(file_path=file_path)
        return e.read_excel(sheet_name=sheet_name)


class MySQL(SqlDB):
    def __init__(self, host, username, password, port, db_name, charset='utf8mb4', conn_package='pymysql', keep_conn=False, jdbc_url=None):
        req_params = list(inspect.signature(super().__init__).parameters.keys())
        run_param = {k: v for k, v in locals().items() if k in req_params}
        run_param['db_type'] = 'mysql'
        super().__init__(**run_param)
        super().set_ext_param(charset=charset)

    def to_db(self, df, tb_name, how, chunksize, fast_load):
        if fast_load:
            super().set_ext_param(charset=self.__charset, local_infile='1')
            tmp_file_path = self.get_tmp_file()
            try:
                df.to_csv(tmp_file_path, index=False, quoting=1)
                sql = f'''
                    LOAD DATA LOCAL INFILE '{tmp_file_path}' INTO TABLE {tb_name} Fields Terminated By ',' Enclosed By '"' IGNORE 1 LINES;
                '''
                self.exe_sql(sql)
            except Exception as e:
                raise Exception(f'Failed to load data into table {tb_name} using fast_load: {e}')
            finally:
                try:
                    os.unlink(tmp_file_path)
                except Exception as e:
                    print(f'Failed to delete temporary file: {e}')
        else:
            super().to_db(df=df, tb_name=tb_name, how=how, chunksize=chunksize)


class PostgresSQL(SqlDB):
    def __init__(self, host, username, password, port, db_name, conn_package='psycopg2', keep_conn=False, jdbc_url=None):
        req_params = list(inspect.signature(super().__init__).parameters.keys())
        run_param = {k: v for k, v in locals().items() if k in req_params}
        run_param['db_type'] = 'postgresql'
        super().__init__(**run_param)


class MSSQL(SqlDB):
    def __init__(self, host, username, password, port, db_name, conn_package='pymssql', keep_conn=False, jdbc_url=None, conn_params=None):
        req_params = list(inspect.signature(super().__init__).parameters.keys())
        run_param = {k: v for k, v in locals().items() if k in req_params}
        run_param['db_type'] = 'mssql'
        super().__init__(**run_param)


class Oracle(SqlDB):
    def __init__(self, host, username, password, port, db_name, conn_package='cx_oracle', keep_conn=False, jdbc_url=None):
        req_params = list(inspect.signature(super().__init__).parameters.keys())
        run_param = {k: v for k, v in locals().items() if k in req_params}
        run_param['db_type'] = 'oracle'
        super().__init__(**run_param)


class Clickhouse(SqlDB):
    def __init__(self, host, username, password, port, db_name, conn_package='native', keep_conn=False, jdbc_url=None):
        self.__host = host
        self.__username = username
        self.__password = quote_plus(password)
        self.__port = port
        self.__db_name = db_name
        req_params = list(inspect.signature(super().__init__).parameters.keys())
        run_param = {k: v for k, v in locals().items() if k in req_params}
        run_param['db_type'] = 'clickhouse'
        super().__init__(**run_param)

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
    def __init__(self, file_path=None, jdbc_url=None, keep_conn=False):
        if jdbc_url is not None:
            pass
        elif file_path is not None:
            jdbc_url = 'sqlite:///%s' % str(file_path)
        else:
            jdbc_url = 'sqlite:///:memory:'
        super().__init__(jdbc_url=jdbc_url, keep_conn=keep_conn)
