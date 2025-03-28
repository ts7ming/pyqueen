import datetime
import os
import warnings

import numpy as np
import pandas as pd
from pyqueen.io.excel import Excel
from pyqueen.utility.time_kit import TimeKit


class DsLog:
    """
    Logger for DataSource
    """

    def __init__(self):
        """
        Logger for DataSource
        """
        self.__t_start = None
        self.etl_log = {}
        self.__etl_param_sort = [
            'py_path',
            'func_name',
            'start_time',
            'end_time',
            'duration',
            'message',
            'file_path',
            'sql_text',
            'server_id',
            'db_name',
            'table_name'
        ]
        self.logger = None
        self.__server_id = None


    def __file_log(self, etl_log):
        """
        std logger for "file", save ds log to file
        :param etl_log: etl log
        :return:
        """
        if self.__log_path is None:
            self.__log_path = etl_log['py_path']
            self.__log_path = str(self.__log_path).replace('.py', '.log')
        info = '\n------------------------------------\n'
        for k, v in etl_log.items():
            info += str(k) + ': ' + str(v) + '\n'
        info += '\n------------------------------------\n'
        with open(self.__log_path, 'a+', encoding='utf-8') as f:
            f.write(info)

    def __db_log(self, etl_log):
        df = pd.DataFrame({k: [v] for k, v in etl_log.items()})
        if not df.empty:
            self.__ds_log.to_db(df, self.__tb_log)

    def set_logger(self, logger, log_path=None, log_ds=None, log_tb=None,auto_create=False, server_id=None):
        """
        reset logger for DataSource
        :param logger: a function
        :return:
        """
        if str(logger) == 'file':
            self.logger = self.__file_log
            if log_path is None:
                warnings.warn("using running script folder", PendingDeprecationWarning)
            self.__log_path = log_path
        elif str(logger) == 'db':
            if log_ds is None or log_tb is None:
                raise ValueError('please provide log_ds and log_tb')
            self.logger = self.__db_log
            self.__ds_log = log_ds
            self.__ds_log.logger = None # 避免循环引用
            self.__tb_log = log_tb
            if auto_create:
                self.create_log_table(log_tb)
        else:
            self.logger = logger
        if server_id is not None:
            self.__server_id = server_id
        else:
            self.__server_id = self.auto_server_id

    def trace_start(self):
        """
        DataSource log
        :return:
        """
        self.__t_start = datetime.datetime.now()
        import inspect
        self.etl_log['start_time'] = str(self.__t_start.strftime('%Y-%m-%d %H:%M:%S'))
        self.etl_log['py_path'] = inspect.stack()[3].filename
        self.etl_log['func_name'] = inspect.stack()[2].function
        self.etl_log['server_id'] = self.__server_id

    def trace_end(self):
        """
        DataSource log
        :return:
        """
        t_end = datetime.datetime.now()
        t_duration = str((t_end - self.__t_start).seconds)
        self.etl_log['end_time'] = str(t_end.strftime('%Y-%m-%d %H:%M:%S'))
        self.etl_log['duration'] = t_duration
        sortd_log = {i: self.etl_log[i] for i in self.__etl_param_sort if i in self.etl_log}
        for k, v in self.etl_log.items():
            if k not in sortd_log:
                sortd_log[k] = v
        if self.logger is not None:
            self.logger(sortd_log)
        self.etl_log = {}

    def create_log_table(self, table_name='ds_log'):
        """
        create log table for DataSource
        """
        base_sql = """
        CREATE TABLE {} (
            id {} NOT NULL AUTO_INCREMENT,
            py_path VARCHAR(500) DEFAULT NULL,
            func_name VARCHAR(100) DEFAULT NULL,
            start_time DATETIME DEFAULT NULL,
            end_time DATETIME DEFAULT NULL,
            duration INT DEFAULT NULL,
            message VARCHAR(500) DEFAULT NULL,
            file_path VARCHAR(500) DEFAULT NULL,
            sql_text VARCHAR(500) DEFAULT NULL,
            server_id VARCHAR(200) DEFAULT NULL,
            db_name VARCHAR(50) DEFAULT NULL,
            table_name VARCHAR(100) DEFAULT NULL,
            PRIMARY KEY (id)
        ) {}
        """

        id_type = ''
        extra = ''
        if self.__ds_log.conn_type == 'mysql':
            id_type = 'BIGINT UNSIGNED'
            extra = 'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
        elif self.__ds_log.conn_type == 'oracle':
            id_type = 'NUMBER(19)'
        elif self.__ds_log.conn_type == 'postgresql':
            id_type = 'SERIAL'
        elif self.__ds_log.conn_type == 'sqlite':
            id_type = 'INTEGER'
            extra = ''
        elif self.__ds_log.conn_type == 'mssql':
            id_type = 'INT'
        else:
            raise ValueError('Unsupported database type: {}'.format(self.__ds_log.conn_type))
        sql = base_sql.format(table_name, id_type, extra)

        if self.__ds_log.conn_type == 'mssql':
            sql = sql.replace('AUTO_INCREMENT', 'IDENTITY(1,1)')

        self.__ds_log.exe_sql(sql)


class DsPlugin:
    """
    基于DataSource的插件
    """

    def row_count(self, table_name):
        """
        quickly get row count of a table
        """
        # 使用参数化查询来避免SQL注入
        sql = 'SELECT COUNT(*) FROM {}'.format(table_name)
        try:
            df = self.read_sql(sql)
            rows = df.iloc[0, 0]
            if isinstance(rows, (int, float)):
                return int(rows)
            else:
                raise ValueError("查询结果无法转换为整数")
        except Exception as e:
            print(f"查询行数时出错: {e}")
            return None


    def get_value(self, sql):
        """
        quickly get first value of a query
        """
        try:
            df = self.read_sql(sql)
            if not df.empty:
                return df.iloc[0, 0]
            else:
                return None
        except Exception as e:
            print(f"An error occurred while executing the SQL query: {e}")
            return None


    def get_sql_group(self, sql, params):
        """
        quickly get all query results
        """
        try:
            dfs = [self.read_sql(sql.format(param)) for param in params]
            df = pd.concat(dfs, ignore_index=True)
            return df
        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()


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
        """
        基于 pandas.DataFrame 生成png图片

        :param font_size: 字体大小
        :param col_width: 列宽: auto: 根据传入 df 自动设置 也可以传入列表指定每列宽度 由plt自动设置,
        :param df: pd.DataFrame对象
        :param file_path: 目标图片路径, 如果为None则自动生成临时路径
        :return:
        """
        import matplotlib.pylab as plt
        import tempfile

        if file_path is None:
            file_path = tempfile.gettempdir() + '/tmp.png'
        pd.set_option('display.unicode.ambiguous_as_wide', True)
        pd.set_option('display.unicode.east_asian_width', True)

        plt.figure()
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.subplots_adjust(top=0.7, bottom=0, left=0, right=1, hspace=3, wspace=3)
        plt.margins(1, 1)
        ax = plt.subplot(111, 1, 1)
        ax.xaxis.set_visible(False)
        ax.yaxis.set_visible(False)

        if col_width == 'auto':
            tmp_list = [max(len(str(a)), len(str(b))) for a, b in
                        zip(list(df.head(1).to_records()[0])[1:], list(df.columns))]
            new_col_width = [x / sum(tmp_list) for x in tmp_list]
            dtable = ax.table(cellText=df.values, colLabels=df.columns, colWidths=new_col_width)
        elif col_width != 'auto' and col_width is not None:
            dtable = ax.table(cellText=df.values, colLabels=df.columns, colWidths=col_width)
        else:
            dtable = ax.table(cellText=df.values, colLabels=df.columns)
        if font_size is not None:
            dtable.auto_set_font_size(False)
            dtable.set_fontsize(font_size)
        plt.savefig(file_path, dpi=600, bbox_inches='tight')
        return file_path


    @staticmethod
    def delete_file(path):
        """
        delete all files in a directory
        """
        try:
            for item in os.listdir(path):
                item_path = os.path.join(path, item)
                try:
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    else:
                        print(f"skip: {item_path}")
                except OSError as e:
                    print(f"error: {item_path}, message: {e}")
        except OSError as e:
            print(f"error: {path}, message: {e}")


    @staticmethod
    def get_tmp_file():
        import tempfile
        _, file_path = tempfile.mkstemp()
        return file_path
    

    def __backup(self, ds, table_name):
        ts = str(TimeKit().now)
        bk_table_name = table_name+"_"+ts
        if self.conn_type in ('mysql','pgsql','sqlite'):
            ds.exe_sql(f"CREATE TABLE {bk_table_name} AS SELECT * FROM {table_name}")
        elif self.conn_type == 'mssql':
            ds.exe_sql(f"SELECT * INTO {bk_table_name} FROM {table_name}")
        elif self.conn_type == 'oracle':
            ds.exe_sql(f"CREATE TABLE {bk_table_name} AS SELECT * FROM {table_name} WHERE 1=2")
            ds.exe_sql(f"INSERT INTO {bk_table_name} SELECT * FROM {table_name}")
        else:
            raise Exception("暂不支持该数据库类型")


    def sync_to_ds(self, ds_target, table_list:list, conf:dict=None, truncate=True, backup=True, tb_rename:dict=None, col_rename:dict=None):
        """
        将当前数据源的数据导入到目标数据源

        :param ds_target: 目标数据源
        :param table_list: 表名列表
        :param conf: 配置信息, 可以指定每个表的配置信息, 包括: backup, truncate, col_rename. 配置信息的优先级高于全局配置
        :param truncate: 是否清空目标表, 默认True
        :param backup: 是否备份目标表, 默认True
        :param tb_rename: 表名映射, 可以指定每个表的映射关系, 包括: old_tb_name: new_tb_name
        :param col_rename: 列名映射, 可以指定每个表的映射关系, 包括: old_col_name: new_col_name

        conf_example = {
            'table_name1':{
                'backup': False,
                'truncate': True,
                'col_rename': {'old_col_name': 'new_col_name'}
            },
            'table_name2':{
                'backup': True,
                'truncate': False,
                'col_rename': {'old_col_name': 'new_col_name'}
            }
        }
        """
        if ds_target.conn_type not in ('mysql','pgsql','sqlite','mssql','oracle'):
            raise Exception("暂不支持该数据库类型")
        for tb in table_list:
            df = self.read_sql(f"select * from {tb}")
            if conf is not None and tb in conf:
                tb_conf = conf[tb]
                if 'backup' in tb_conf:
                    backup = tb_conf['backup']
                if 'truncate' in tb_conf:
                    truncate = tb_conf['truncate']
                if 'col_rename' in tb_conf:
                    col_rename = tb_conf['col_rename']
            if backup:
                self.__backup(ds_target,tb)
            if truncate:
                ds_target.exe_sql("truncate table {}".format(tb))
            if col_rename is not None:
                df.rename(columns=col_rename,inplace=True)
            if tb_rename is not None and tb in tb_rename:
                tb = tb_rename[tb]
            ds_target.to_db(df, tb)


    def generate_ddl(self, base):
        self._create_conn()
        engine = self._get_engine()
        from sqlalchemy.schema import CreateTable
        ddl_statements = []
        for table_name in base.metadata.tables:
            table = base.metadata.tables[table_name]
            ddl = str(CreateTable(table).compile(engine))
            ddl_statements.append(ddl)
        self._close_conn()
        return ddl_statements


class DsConfig:
    """
    仅用于兼容老代码
    """

    def set_chunksize(self, chunksize):
        pass

    def set_charset(self, charset):
        pass

    def set_package(self, package_name):
        pass

    def set_cache_dir(self, cache_dir):
        pass

    def set_url(self, url):
        pass

    def keep_conn(self):
        pass

    def close_conn(self):
        pass




