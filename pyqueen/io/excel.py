import pandas as pd
import numpy as np
import os


class Excel:
    def __init__(self, file_path=None):
        self.__ds = None
        if file_path is None:
            file_path = 'pyqueen_excel'
        self.file_path = self.__check_path(file_path=file_path)

    @staticmethod
    def __check_path(file_path):
        if os.path.isabs(file_path) and str(file_path)[-5:] == '.xlsx':
            pass
        elif os.path.isabs(file_path) is False and str(file_path)[-5:] != '.xlsx':
            file_path = os.path.join(os.path.curdir, file_path) + '.xlsx'
        else:
            file_path = os.path.join(os.path.curdir, file_path)
        return file_path

    def read_excel(self, sheet_name=None, file_path=None):
        if file_path is not None:
            file_path = self.__check_path(file_path)
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        else:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name)
        return df

    def read_sql(self, sql, data=None, engine='sqlite'):
        from pyqueen import DataSource
        if engine == 'sqlite':
            self.__ds = DataSource(conn_type='sqlite', keep_conn=True)
            if data is None:
                data = pd.read_excel(self.file_path, sheet_name=None)
            for sht_name, df in data.items():
                self.__ds.to_db(df, sht_name)
            df_result = self.__ds.read_sql(sql)
            self.__ds.close_conn()
        elif engine == 'duckdb':
            import duckdb
            with duckdb.connect() as conn:
                for df_name, df in data.items():
                    conn.register(df_name, df)
                df_result = conn.execute(sql).df()
        else:
            raise Exception('wrong engine')
        return df_result

    def to_excel(self, sheet_list, file_path=None, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11, column_width=17):
        if file_path is not None:
            file_path = self.__check_path(file_path)
        else:
            file_path = self.file_path

        import xlsxwriter
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
        return file_path
