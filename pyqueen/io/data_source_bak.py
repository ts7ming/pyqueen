import datetime
import pandas as pd
from pyqueen.io.excel import Excel


class DataSource:
    def __init__(self, host=None, username=None, password=None, port=None, db_name=None, db_type='MySQL'):
        if str(db_type).lower() in ('mysql', 'mssql', 'oracle', 'clickhouse', 'sqlite', 'postgresql', 'pgsql'):
            from pyqueen.io.db import DB
            self.__db = DB(host=host, username=username, password=password, port=port, db_name=db_name, db_type=db_type)
        if str(db_type).lower() == 'ftp':
            from pyqueen.io.ftp import FTP
            self.__ftp = FTP(username=username, password=password, host=host, port=port)
        self.__excel = Excel()
        self.__logger = None
        self.__host = host
        self.__username = username
        self.__port = port
        self.__db_name = db_name
        self.__db_type = db_type
        self.__etl_log = {}
        self.__cache_dir = None
        self.__etl_param_sort = [
            'py_path',
            'func_name',
            'start_time',
            'end_time',
            'duration',
            'message',
            'file_path',
            'sql_text',
            'host',
            'db_type',
            'port',
            'db_name',
            'table_name'
        ]

    def import_test_data(self, excel_path):
        from pyqueen.io.db import DB
        self.__db = DB(host=':memory:', db_name='main', db_type='sqlite')
        self.__db.keep_conn()
        data = pd.read_excel(excel_path, sheet_name=None)
        for sht_name, df in data.items():
            self.__db.to_db(df, sht_name)

    @staticmethod
    def __file_log(etl_log):
        log_path = etl_log['py_path']
        log_path = str(log_path).replace('.py', '.log')
        info = '\n------------------------------------\n'
        for k, v in etl_log.items():
            info += str(k) + ': ' + str(v) + '\n'
        info += '\n------------------------------------\n'
        with open(log_path, 'a+', encoding='utf-8') as f:
            f.write(info)

    def set_logger(self, logger=print):
        if str(logger) == 'file':
            self.__logger = self.__file_log
        else:
            self.__logger = logger

    def set_db(self, db_name):
        self.__db_name = db_name
        if self.__db_type != 'sqlite':
            self.__db.set_db(db_name=db_name)

    def set_chunksize(self, chunksize):
        if self.__logger is not None:
            self.__start()
            self.__etl_log['message'] = '设置chunksize为: ' + str(chunksize)
            self.__end()
        self.__db.set_chunksize(chunksize=chunksize)

    def set_charset(self, charset):
        if self.__logger is not None:
            self.__start()
            self.__etl_log['message'] = '设置字符集为: ' + str(charset)
            self.__end()
        self.__db.set_charset(charset=charset)

    def set_package(self, package_name):
        self.__db.set_package(package_name)

    def set_cache_dir(self, cache_dir):
        self.__cache_dir = cache_dir

    def set_url(self, url):
        self.__db.set_url(url)

    def keep_conn(self):
        self.__db.keep_conn()

    def close_conn(self):
        self.__db.close_conn()

    def get_sql(self, sql):
        """
        查询 SQL 返回 DataFrame 对象
        :param sql: 待查询 sql
        :return:
        """
        if self.__logger is not None:
            self.__start()
            self.__etl_log['sql_text'] = str(sql).strip('\n').strip(' ')
            self.__etl_log['host'] = self.__host
            self.__etl_log['port'] = str(self.__port)
            self.__etl_log['db_name'] = self.__db_name
            self.__etl_log['db_type'] = self.__db_type
        ret = self.__db.get_sql(sql)
        if self.__logger is not None:
            self.__end()
        return ret

    def get_value(self, sql):
        """
        查询 SQL 返回结果的第一个值
        用于取汇总信息
        :param sql: 待查询 sql
        :return:
        """
        if self.__logger is not None:
            self.__start()
            self.__etl_log['sql_text'] = str(sql).strip('\n').strip(' ')
            self.__etl_log['host'] = self.__host
            self.__etl_log['port'] = str(self.__port)
            self.__etl_log['db_name'] = self.__db_name
            self.__etl_log['db_type'] = self.__db_type
        ret = self.__db.get_value(sql)
        if self.__logger is not None:
            self.__end()
        return ret

    def to_db(self, df, tb_name: str, fast_load: str = False, how: str = 'append'):
        """
        DataFrame 对象写入数据库
        :param df: 待写入数据, 字段名需和数据库一致
        :param tb_name: 表名
        :param fast_load: 是否快速导入模式, 仅支持 mysql
        :param how: append/replace
        """
        if self.__logger is not None:
            self.__start()
            self.__etl_log['table_name'] = tb_name
            self.__etl_log['host'] = self.__host
            self.__etl_log['port'] = str(self.__port)
            self.__etl_log['db_name'] = self.__db_name
            self.__etl_log['db_type'] = self.__db_type
        self.__db.to_db(df=df, tb_name=tb_name, fast_load=fast_load, how=how)
        if self.__logger is not None:
            self.__end()

    def exe_sql(self, sql):
        if self.__logger is not None:
            self.__start()
            self.__etl_log['sql_text'] = str(sql).strip('\n').strip(' ')
            self.__etl_log['host'] = self.__host
            self.__etl_log['port'] = str(self.__port)
            self.__etl_log['db_name'] = self.__db_name
            self.__etl_log['db_type'] = self.__db_type
        self.__db.exe_sql(sql)
        if self.__logger is not None:
            self.__end()

    def get_tmp_file(self):
        return self.__db.get_tmp_file()

    def delete_file(self, path):
        if self.__logger is not None:
            self.__start()
            self.__etl_log['file_path'] = path
        self.__excel.delete_file(path)
        if self.__logger is not None:
            self.__end()

    def to_excel(self,
                 file_path,
                 sheet_list,
                 fillna='',
                 fmt=None,
                 font='微软雅黑',
                 font_color='black',
                 font_size=11,
                 column_width=17
                 ):
        """
        DataFrame对象写入Excel文件路径不存在时自动创建
        file_path: 文件路径 (须以 .xlsx结尾)
        sheet_list: list [[dataframe,sheet_name],[dataframe2,sheet_name2]]

        fmt={
            'col1':'#,##0',
            'col2':'#,##0.0',
            'col3':'0%',
            'col4':'0.00%',
            'col5':'YYYY-MM-DD',
            ''
        }
        """
        if self.__logger is not None:
            self.__start()
            self.__etl_log['file_path'] = file_path
        self.__excel.to_excel(
            file_path=file_path,
            sheet_list=sheet_list,
            fillna=fillna, fmt=fmt,
            font=font,
            font_color=font_color,
            font_size=font_size,
            column_width=column_width
        )
        if self.__logger is not None:
            self.__end()

    def read_excel(self, path, sheet_name=None):
        if self.__logger is not None:
            self.__start()
            self.__etl_log['file_path'] = path
        self.__excel.read_excel(path=path, sheet_name=sheet_name)
        if self.__logger is not None:
            self.__end()

    def download_ftp(self, local_dir, remote_dir):
        if self.__logger is not None:
            self.__start()
            self.__etl_log['file_path'] = remote_dir
            self.__etl_log['host'] = self.__host
            self.__etl_log['port'] = str(self.__port)
            self.__etl_log['db_type'] = self.__db_type
        self.__ftp.download_folder(local_dir=local_dir, remote_dir=remote_dir)
        if self.__logger is not None:
            self.__end()

    def __start(self):
        self.__t_start = datetime.datetime.now()
        import inspect
        a = inspect.stack()[2]
        file_name = a.filename
        func = a.function
        self.__etl_log['start_time'] = str(self.__t_start.strftime('%Y-%m-%d %H:%M:%S'))
        self.__etl_log['py_path'] = file_name
        self.__etl_log['func_name'] = func

    def __end(self):
        t_end = datetime.datetime.now()
        t_duration = str((t_end - self.__t_start).seconds)
        self.__etl_log['end_time'] = str(t_end.strftime('%Y-%m-%d %H:%M:%S'))
        self.__etl_log['duration'] = t_duration
        sortd_log = {i: self.__etl_log[i] for i in self.__etl_param_sort if i in self.__etl_log}
        for k, v in self.__etl_log.items():
            if k not in sortd_log:
                sortd_log[k] = v
        if self.__logger is not None:
            self.__logger(sortd_log)
        self.__etl_log = {}

    @staticmethod
    def pdsql(sql, data):
        import duckdb
        with duckdb.connect() as conn:
            for df_name, df in data.items():
                conn.register(df_name, df)
            result = conn.execute(sql).df()
        return result

    @staticmethod
    def to_image(df, file_path=None, col_width=None, font_size=None):
        from pyqueen.io.image import Image
        path = Image.df2image(df, file_path=file_path, col_width=col_width, font_size=font_size)
        return path

    def get_web(self, url):
        from pyqueen.io.web import Web
        w = Web(cache_dir=self.__cache_dir)
        return w.read_page(url)