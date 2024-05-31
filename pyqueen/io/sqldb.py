import os
from sqlalchemy import create_engine, text
import pandas as pd
import inspect
from urllib.parse import quote_plus

class SqlDB:
    def __init__(self, conn_package=None, db_type=None, host=None, username=None, password=None, port=None, db_name=None, jdbc_url=None,
                 keep_conn=False, conn_params=None):
        self.__conn = None
        self.__engine = None
        self.__base_param = {
            'db_type': db_type,
            'package': str(conn_package),
            'username': str(username),
            'password': quote_plus(password),
            'host': str(host),
            'port': str(port),
            'db_name': str(db_name),
        }
        self.__url = jdbc_url
        self.__ext_param = None
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

    def create_conn(self):
        if self.__keep_conn is False or self.__conn is None:
            self.__build_url()
            self.__engine = create_engine(self.__url)
            self.__conn = self.__engine.connect()

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
        if auto_commit:
            self.__build_url()
            engine = create_engine(self.__url)
            engine = engine.execution_options(isolation_level="AUTOCOMMIT")
            conn = engine.connect()
        else:
            self.create_conn()
            engine = self.__engine
            conn = self.__conn

        if isinstance(sql, list):
            try:
                for sql_text in sql:
                    sql_text = text(sql_text)
                    conn.execute(sql_text)
                    conn.commit()
            except Exception as e:
                raise Exception('执行sql出错: ' + str(e)[0:500])
            finally:
                self.close_conn(engine=engine, conn=conn)
        else:
            sql = text(sql)
            try:
                conn.execute(sql)
                conn.commit()
            except Exception as e:
                raise Exception('执行sql出错: ' + str(e)[0:500])
            finally:
                self.close_conn(engine=engine, conn=conn)

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
