from pyqueen.ds.excel import Excel


class DataSource:
    def __init__(self, host=None, username=None, password=None, port=None, db_name=None, db_type='MySQL'):
        if str(db_type).lower() in ('mysql', 'mssql', 'oracle', 'clickhouse', 'sqlite'):
            from pyqueen.ds.db import DB
            self.__db = DB(host=host, username=username, password=password, port=port, db_name=db_name, db_type=db_type)
        if str(db_type).lower() == 'ftp':
            from ftp import FTP
            self.__ftp = FTP(username=username, password=password, host=host, port=port)
        self.excel = Excel()

    def get_sql(self, sql):
        return self.__db.get_sql(sql)

    def get_value(self, sql):
        return self.__db.get_value(sql)

    def to_db(self, df, tb_name: str, fast_load: str = False, how: str = 'append'):
        self.__db.to_db(df=df, tb_name=tb_name, fast_load=fast_load, how=how)

    def exe_sql(self, sql):
        self.__db.exe_sql(sql)

    def get_tmp_file(self):
        return self.__db.get_tmp_file()

    def delete_file(self, path):
        self.excel.delete_file(path)

    def to_excel(self, file_path, sheet_list, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11, column_width=17):
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
        self.excel.read_excel(path=path, sheet_name=sheet_name)

    def download_ftp(self, local_dir, remote_dir):
        self.__ftp.download_folder(local_dir=local_dir, remote_dir=remote_dir)
