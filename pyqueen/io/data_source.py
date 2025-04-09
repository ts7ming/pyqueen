import inspect
import os
import sys
import warnings
from pyqueen.io.ds_plugin import DsLog, DsPlugin, DsConfig
from pyqueen.io.sqldb import *
from pyqueen.io.kvdb import *
from pyqueen.io.excel import *
from pyqueen.io.ftp import *
from pyqueen.io.web import *


sys.path.append(os.path.dirname(os.path.abspath(__file__)))
warnings.simplefilter(action='always', category=PendingDeprecationWarning)

__conn_type_mapping__ = {
    'mysql': MySQL,
    'oracle': Oracle,
    'mssql': MSSQL,
    'clickhouse': Clickhouse,
    'pgsql': PostgresSQL,
    'sqlite': Sqlite,
    'jdbc': SqlDB,
    'redis': KvDB,
    'excel': Excel,
    'ftp': FTP,
    'web': Web
}
__support_conn_type__ = tuple(__conn_type_mapping__.keys())


class DataSource(DsLog, DsPlugin, DsConfig):
    def __init__(self,
                 conn_type,
                 host=None,
                 username=None,
                 password=None,
                 port=None,
                 db_name=None,
                 db_type=None,
                 file_path=None,
                 jdbc_url=None,
                 cache_dir=None,
                 keep_conn=False,
                 charset=None,
                 conn_package=None,
                 conn_params=None
                 ):
        super().__init__()
        self.auto_server_id = '[' + str(username) + ']@['+str(host)+']:['+str(port)+']'
        self.conn_type = str(conn_type).lower()
        self.__init_params = {k: v for k, v in locals().items()}
        
        if self.conn_type is None and db_type is None:
            raise ValueError("conn_type must be specified")
        
        if self.conn_type not in __support_conn_type__:
            raise ValueError(self.conn_type + " is not supported")

        if db_type is not None and self.conn_type is None:
            warnings.warn("Recommend using the 'conn_type' field instead of 'db_type'", PendingDeprecationWarning)
            self.conn_type = db_type

        if conn_type == 'sqlite' and host is not None and file_path is None:
            self.__init_params['file_path'] = host


        self.conn_type = str(conn_type).lower()
        self.operator_class = __conn_type_mapping__[self.conn_type]
        self.__build_conn()

    def __build_conn(self):
        req_params = inspect.signature(self.operator_class).parameters.keys()
        run_param = {k: self.__init_params[k] for k in req_params if k in self.__init_params and self.__init_params[k] is not None}
        self.operator = self.operator_class(**run_param)

    def _get_engine(self):
        return self.operator._get_engine()
    
    def _create_conn(self):
        try:
            self.operator.create_conn()
        except Exception as e:
            print(e)

    def _close_conn(self):
        try:
            self.operator.close_conn()
        except Exception as e:
            print(e)

    def __run(self, log_field, **kwargs):
        func_name = inspect.currentframe().f_back.f_code.co_name
        func = getattr(self.operator, func_name)
        if self.logger is not None and log_field is not None:
            self.trace_start()
            for fd in log_field:
                fd_v = None
                if fd in kwargs.keys():
                    fd_v = kwargs[fd]
                elif fd in self.__init_params.keys():
                    fd_v = self.__init_params[fd]
                else:
                    continue
                fd_v = str(fd_v).strip('\n').strip(' ')
                if fd == 'sql':
                    fd = 'sql_text'
                elif fd == 'tb_name':
                    fd = 'table_name'
                self.etl_log[fd] = fd_v
                
        req_params = list(inspect.signature(func).parameters.keys())
        run_param = {k: v for k, v in kwargs.items() if k in req_params}
        ret = func(**run_param)
        if self.logger is not None and log_field is not None:
            self.trace_end()
        return ret

    def set(self, param, val):
        """
        set parameter for this datasource
        :param param: parameter name
        :param val: parameter value
        :return:
        """
        self.__init_params[param] = val
        self.__build_conn()

    def set_db(self, db_name):
        """
        reset current database
        :param db_name: database name
        :return:
        """
        self.set('db_name', db_name)

    def get_sql(self, sql):
        """
        get data from sql query
        :param sql: sql text
        :return: pd.DataFrame
        """
        return self.read_sql(sql)

    def read_sql(self, sql, data=None, engine='sqlite'):
        """
        get data from sql query
        :param data: query on dataframe
        :param engine: use sqlite or duckdb
        :param sql: sql text
        :return: pd.DataFrame
        """
        log_field = ['sql', 'host', 'port', 'db_name', 'conn_type']
        ret = self.__run(log_field=log_field, sql=sql, data=data, engine=engine)
        return ret

    def exe_sql(self, sql, auto_commit=False):
        """
        execute sql on server
        :param auto_commit:
        :param sql: str or list; if list execute every sql in list
        :return:
        """
        log_field = ['sql', 'host', 'port', 'db_name', 'conn_type']
        ret = self.__run(log_field=log_field, sql=sql, auto_commit=auto_commit)
        return ret

    def to_db(self, df, tb_name, how='append', fast_load=False, chunksize=10000):
        """
        write a pd.DataFrame to database
        :param df: df to write
        :param tb_name: table name
        :param how: append or replace
        :param fast_load: only support mysql and clickhouse. df to csv to database
        :param chunksize: chunksize
        :return:
        """
        log_field = ['tb_name', 'host', 'port', 'db_name', 'conn_type']
        ret = self.__run(log_field=log_field, df=df, tb_name=tb_name, how=how, fast_load=fast_load, chunksize=chunksize)
        return ret

    def get_v(self, key):
        """
        get value by key from kv database
        :param key: key
        :return: value
        """
        log_field = ['key', 'host', 'port', 'db_name', 'conn_type']
        ret = self.__run(log_field=log_field, key=key)
        return ret

    def set_v(self, key, value):
        """
        set value for a key on kv database
        :param key: key
        :param value: value
        :return:
        """
        log_field = ['key', 'value', 'host', 'port', 'db_name', 'conn_type']
        ret = self.__run(log_field=log_field, key=key, value=value)
        return ret

    def read_excel(self, sheet_name=None, file_path=None):
        """
        read an excel file to pd.DataFrame
        :param sheet_name: sheet_name
        :param file_path: file_path, if None use self.file_path
        :return: pd.DataFrame
        """
        log_field = ['sheet_name', 'file_path', 'conn_type']
        ret = self.__run(log_field=log_field, sheet_name=sheet_name, file_path=file_path)
        return ret

    def to_excel(self, sheet_list, file_path=None, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11, column_width=17):
        """
        write a pd.DataFrame to excel file
        :param sheet_list: [[df1, sht_name1],[df2, sht_name2],...]
        :param file_path: if None, use self.file_path
        :param fillna: string to fill NA value
        :param fmt: set excel cell format by column name
        :param font: font
        :param font_color: font color
        :param font_size: font color
        :param column_width: column_width
        :return:
        """
        log_field = ['file_path', 'conn_type']
        ret = self.__run(log_field=log_field, sheet_list=sheet_list, file_path=file_path, fillna=fillna, fmt=fmt, font=font,
                         font_color=font_color, font_size=font_size, column_width=column_width)
        return ret

    def download_dir(self, local_dir, remote_dir):
        """
        download entire dir from ftp server
        :param local_dir: local dir
        :param remote_dir: remote dir
        :return:
        """
        log_field = ['local_dir', 'remote_dir', 'host', 'port', 'username', 'conn_type']
        ret = self.__run(log_field=log_field, local_dir=local_dir, remote_dir=remote_dir)
        return ret

    def read_page(self, url):
        """
        get source code of a page by url
        :param url: url
        :return: page
        """
        log_field = None
        ret = self.__run(log_field=log_field, url=url)
        return ret
