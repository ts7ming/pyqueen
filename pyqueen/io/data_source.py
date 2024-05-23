import warnings
import pandas as pd
from pyqueen.io.ds_plugin import DsLog, DsPlugin, DsConfig, DsExt

warnings.simplefilter(action='always', category=PendingDeprecationWarning)

__conn_type_mapping__ = {
    'mysql': 'MySQL',
    'oracle': 'Oracle',
    'mssql': 'MSSQL',
    'clickhouse': 'Clickhouse',
    'pgsql': 'PostgresSQL',
    'sqlite': 'Sqlite',
    'jdbc': 'SqlDB',
    'redis': 'KvDB',
    'excel': 'Excel',
    'ftp': 'Ftp',
    'web': 'Web'
}

__support_conn_type__ = tuple(__conn_type_mapping__.keys())
print(','.join(__support_conn_type__))


class DataSource(DsLog, DsPlugin, DsConfig, DsExt):
    def __init__(self,
                 conn_type=None,
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
                 conn_package=None
                 ):
        super().__init__()
        if conn_type is None and db_type is None:
            raise Exception('missing conn_type! supported conn_type:' + ','.join(__support_conn_type__))
        if conn_type is None and db_type is not None:
            warnings.warn(message="recommend using the 'conn_type' field instead of 'db_type'", category=PendingDeprecationWarning)
            conn_type = db_type
        conn_type = str(conn_type).lower()
        operator = __conn_type_mapping__[conn_type]
        if conn_type in ('mysql', 'mssql', 'oracle', 'clickhouse', 'sqlite', 'postgresql', 'pgsql', 'jdbc'):
            from pyqueen.io import sqldb
            self.operator = getattr(sqldb, operator)(
                host=host,
                username=username,
                password=password,
                port=port,
                db_name=db_name,
                jdbc_url=jdbc_url,
                keep_conn=keep_conn,
                charset=charset,
                conn_package=conn_package
            )
        elif conn_type in ('excel',):
            from pyqueen.io import excel
            self.operator = getattr(excel, operator)(file_path=file_path)
        elif conn_type in ('redis',):
            from pyqueen.io import kvdb
            self.operator = getattr(kvdb, operator)(conn_type=conn_type, host=host, port=port, db_name=db_name, keep_conn=keep_conn)
        elif conn_type in ('ftp',):
            from pyqueen.io import ftp
            self.operator = getattr(ftp, operator)(conn_type=conn_type, host=host, port=port, username=username, password=password, encoding='utf-8')
        elif conn_type in ('web',):
            from pyqueen.io import web
            self.operator = getattr(web, operator)(conn_type=conn_type, cache_dir=cache_dir)
        else:
            raise Exception('Unknown conn_type')

        self.__conn_type = conn_type
        self.__host = host
        self.__username = username
        self.__password = password
        self.__port = port
        self.__db_name = db_name
        self.__file_path = file_path
        self.__jdbc_url = jdbc_url

    def set_db(self, db_name):
        self.operator.set_db(db_name=db_name)

    def get_sql(self, sql):
        return self.read_sql(sql)

    def read_sql(self, sql):
        if self.logger is not None:
            self.trace_start()
            self.etl_log['sql_text'] = str(sql).strip('\n').strip(' ')
            self.etl_log['host'] = self.__host
            self.etl_log['port'] = str(self.__port)
            self.etl_log['db_name'] = self.__db_name
            self.etl_log['conn_type'] = self.__conn_type
        ret = self.operator.read_sql(sql)
        if self.logger is not None:
            self.trace_end()
        return ret

    def exe_sql(self, sql):
        if self.logger is not None:
            self.trace_start()
            self.etl_log['sql_text'] = str(sql).strip('\n').strip(' ')
            self.etl_log['host'] = self.__host
            self.etl_log['port'] = str(self.__port)
            self.etl_log['db_name'] = self.__db_name
            self.etl_log['conn_type'] = self.__conn_type
        self.operator.exe_sql(sql)
        if self.logger is not None:
            self.trace_end()

    def to_db(self, df, tb_name, how='append', fast_load=False, chunksize=10000):
        if self.logger is not None:
            self.trace_start()
            self.etl_log['table_name'] = tb_name
            self.etl_log['host'] = self.__host
            self.etl_log['port'] = str(self.__port)
            self.etl_log['db_name'] = self.__db_name
            self.etl_log['conn_type'] = self.__conn_type
        self.operator.to_db(df=df, tb_name=tb_name, how=how, fast_load=fast_load, chunksize=chunksize)
        if self.logger is not None:
            self.trace_end()

    def get_v(self, key):
        if self.logger is not None:
            self.trace_start()
            self.etl_log['key'] = str(key).strip('\n').strip(' ')
            self.etl_log['host'] = self.__host
            self.etl_log['port'] = str(self.__port)
            self.etl_log['db_name'] = self.__db_name
            self.etl_log['conn_type'] = self.__conn_type
        ret = self.operator.get_v(key)
        if self.logger is not None:
            self.trace_end()
        return ret

    def set_v(self, key, value):
        if self.logger is not None:
            self.trace_start()
            self.etl_log['key'] = str(key)
            self.etl_log['value'] = str(value)
            self.etl_log['host'] = self.__host
            self.etl_log['port'] = str(self.__port)
            self.etl_log['db_name'] = self.__db_name
            self.etl_log['conn_type'] = self.__conn_type
        self.operator.set_v(key, value)
        if self.logger is not None:
            self.trace_end()

    def read_excel(self, sheet_name, file_path=None):
        if file_path is None:
            file_path = self.__file_path
        if self.logger is not None:
            self.trace_start()
            self.etl_log['sheet_name'] = str(sheet_name).strip('\n').strip(' ')
            self.etl_log['file_path'] = file_path
            self.etl_log['conn_type'] = self.__conn_type
        ret = self.operator.read_excel(sheet_name)
        if self.logger is not None:
            self.trace_end()
        return ret

    def to_excel(self, sheet_list, file_path=None, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11, column_width=17):
        if self.logger is not None:
            self.trace_start()
            self.etl_log['file_path'] = str(file_path)
            self.etl_log['conn_type'] = self.__conn_type
        self.operator.to_excel(sheet_list, file_path, fillna, fmt, font, font_color, font_size, column_width)
        if self.logger is not None:
            self.trace_end()

    def download_dir(self, local_dir, remote_dir):
        if self.logger is not None:
            self.trace_start()
            self.etl_log['local_dir'] = str(local_dir).strip('\n').strip(' ')
            self.etl_log['remote_dir'] = str(remote_dir).strip('\n').strip(' ')
            self.etl_log['host'] = self.__host
            self.etl_log['port'] = str(self.__port)
            self.etl_log['username'] = self.__username
            self.etl_log['conn_type'] = self.__conn_type
        self.operator.download_dir(local_dir, remote_dir)
        if self.logger is not None:
            self.trace_end()

    def read_page(self, url):
        return self.operator.read_page(url=url)
