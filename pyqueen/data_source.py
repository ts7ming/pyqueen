import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from pymysql.constants import CLIENT
import pymysql
import os
from urllib.parse import quote_plus
import xlsxwriter

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class DataSource(object):
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

    def get_sql_group(self, sql, params):
        df = None
        for param in params:
            tsql = sql.format(param)
            df_tmp = self.get_sql(tsql)
            if df is None:
                df = df_tmp
            else:
                df = pd.concat([df, df_tmp])
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
            except Exception as e:
                raise Exception('执行sql出错: ' + str(e)[0:500])
            finally:
                cursor.close()
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

    def row_count(self, table_name):
        sql = 'select count(1) from ' + table_name
        df = self.get_sql(sql)
        rows = df.values[0][0]
        return int(rows)

    @staticmethod
    def read_excel(path, sheet_name=None):
        df = pd.read_excel(path, sheet_name=sheet_name)
        return df

    @staticmethod
    def to_excel(file_path, sheet_list, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11,column_width=17):

        '''
        **DataFrame对象写入Excel文件**
        路径不存在时自动创建
        :param file_path: 文件路径 (须以 .xlsx结尾)
        :param sheet_list: list [[dataframe,sheet_name],[dataframe2,sheet_name2]]

        fmt={
            'col1':'#,##0',
            'col2':'#,##0.0',
            'col3':'0%',
            'col4':'0.00%',
            'col5':'YYYY-MM-DD',
            ''
        }
        '''
        if str(file_path)[-5:] != '.xlsx':
            raise Exception('文件路径必须 .xlsx 结尾')
        if os.path.exists(os.path.dirname(file_path)) is False:
            os.makedirs(os.path.dirname(file_path))
        wb = xlsxwriter.Workbook(file_path)
        fmt_default = wb.add_format()
        fmt_default.set_font_name(font)
        fmt_default.set_font_color(font_color)
        fmt_default.set_font_size(font_size)

        style = {
            'align': 'center',
            'bold': True
        }
        fmt_head = wb.add_format(style)
        fmt_head.set_font_name(font)
        fmt_head.set_font_color(font_color)
        fmt_head.set_font_size(font_size)

        if fmt is None:
            fmt = {}
        for df, sheet_name in sheet_list:
            df = df.fillna(fillna)
            df.replace(-np.inf, fillna, inplace=True)
            df.replace(np.inf, fillna, inplace=True)
            ws = wb._add_sheet(sheet_name)
            head = df.columns.to_list()
            data = df.values.tolist()

            row_num, col_num = df.shape
            for col_name, col_index in zip(head, range(col_num)):
                if col_name in fmt.keys():
                    col_format = None
                    col_format = wb.add_format({'num_format': fmt[col_name]})
                    col_format.set_font_name(font)
                    col_format.set_font_color(font_color)
                    col_format.set_font_size(font_size)
                else:
                    col_format = fmt_default
                ws.set_column(col_index, col_index, column_width)
                ws.write(0, col_index, str(col_name), fmt_head)
                for row_index in range(row_num):
                    value = data[row_index][col_index]
                    ws.write(row_index + 1, col_index, value, col_format)
        wb.close()

    @staticmethod
    def delete_file(path):
        try:
            ls = os.listdir(path)
            for i in ls:
                c_path = os.path.join(path, i)
                try:
                    os.remove(c_path)
                except Exception as e:
                    pass
        except Exception as e:
            pass