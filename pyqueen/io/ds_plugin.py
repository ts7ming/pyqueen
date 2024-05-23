import datetime
import os

import pandas as pd


class DsLog:
    def __init__(self):
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
            'host',
            'db_type',
            'port',
            'db_name',
            'table_name'
        ]
        self.logger = None

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

    def set_logger(self, logger):
        if str(logger) == 'file':
            self.logger = self.__file_log
        else:
            self.logger = logger

    def trace_start(self):
        self.__t_start = datetime.datetime.now()
        import inspect
        a = inspect.stack()[2]
        file_name = a.filename
        func = a.function
        self.etl_log['start_time'] = str(self.__t_start.strftime('%Y-%m-%d %H:%M:%S'))
        self.etl_log['py_path'] = file_name
        self.etl_log['func_name'] = func

    def trace_end(self):
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


class DsPlugin:
    def read_sql(self, sql):
        return pd.DataFrame()

    def row_count(self, table_name):
        sql = 'select count(1) from ' + table_name
        df = self.read_sql(sql)
        rows = df.values[0][0]
        return int(rows)

    def get_sql_group(self, sql, params):
        df = None
        for param in params:
            new_sql = sql.format(param)
            df_tmp = self.read_sql(new_sql)
            if df is None:
                df = df_tmp
            else:
                df = pd.concat([df, df_tmp])
        return df


class DsConfig:
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


class DsExt:
    @staticmethod
    def pdsql(sql, data):
        import duckdb
        with duckdb.connect() as conn:
            for df_name, df in data.items():
                conn.register(df_name, df)
            result = conn.execute(sql).df()
        return result

    @staticmethod
    def to_images(df, file_path=None, col_width=None, font_size=None):
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

    @staticmethod
    def get_tmp_file():
        import tempfile
        _, file_path = tempfile.mkstemp()
        return file_path
