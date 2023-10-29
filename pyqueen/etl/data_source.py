"""
DataSource: 统一的额数据源管理

将数据库,Excel文件,FTP文件服务集成到 DataSource
"""

from pyqueen.etl.excel import Excel


class DataSource:
    def __init__(self, host=None, username=None, password=None, port=None, db_name=None, db_type='MySQL'):
        if str(db_type).lower() in ('mysql', 'mssql', 'oracle', 'clickhouse', 'sqlite'):
            from pyqueen.etl.db import DB
            self.__db = DB(host=host, username=username, password=password, port=port, db_name=db_name, db_type=db_type)
        if str(db_type).lower() == 'ftp':
            from ftp import FTP
            self.__ftp = FTP(username=username, password=password, host=host, port=port)
        self.excel = Excel()

    def get_sql(self, sql):
        """
        查询 SQL 返回 DataFrame 对象
        :param sql: 待查询 sql
        :return:
        """
        return self.__db.get_sql(sql)

    def get_value(self, sql):
        """
        查询 SQL 返回结果的第一个值
        用于取汇总信息
        :param sql: 待查询 sql
        :return:
        """
        return self.__db.get_value(sql)

    def to_db(self, df, tb_name: str, fast_load: str = False, how: str = 'append'):
        """
        DataFrame 对象写入数据库
        :param df: 待写入数据, 字段名需和数据库一致
        :param tb_name: 表名
        :param fast_load: 是否快速导入模式, 仅支持 mysql
        :param how: append/replace
        """
        self.__db.to_db(df=df, tb_name=tb_name, fast_load=fast_load, how=how)

    def exe_sql(self, sql):
        """
        执行 SQL
        :param sql: 待执行 sql
        :return:
        """
        self.__db.exe_sql(sql)

    def get_tmp_file(self):
        """
        调用系统函数生成临时文件名
        :return:
        """
        return self.__db.get_tmp_file()

    def delete_file(self, path):
        """
        删除文件
        :param path:
        :return:
        """
        self.excel.delete_file(path)

    def to_excel(self, file_path, sheet_list, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11, column_width=17):
        '''
        DataFrame对象写入Excel文件
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
        self.excel.to_excel(
            file_path=file_path,
            sheet_list=sheet_list,
            fillna=fillna, fmt=fmt,
            font=font,
            font_color=font_color,
            font_size=font_size,
            column_width=column_width
        )

    def read_excel(self, path, sheet_name=None):
        """
        读取excel文件到 DataFrame
        :param path: 文件路径
        :param sheet_name: sheet名
        :return:
        """
        self.excel.read_excel(path=path, sheet_name=sheet_name)

    def download_ftp(self, local_dir, remote_dir):
        """
        下载ftp文件
        整个文件夹下载
        :param local_dir: 本地目录
        :param remote_dir: 远程目录
        :return:
        """
        self.__ftp.download_folder(local_dir=local_dir, remote_dir=remote_dir)
