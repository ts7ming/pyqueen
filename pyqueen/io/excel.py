import pandas as pd
import numpy as np
import os


class Excel:
    def __init__(self, file_path=None):
        if str(file_path)[-5:] != '.xlsx':
            self.file_path = file_path + '.xlsx'
        else:
            self.file_path = file_path

    def read_excel(self, sheet_name=None, file_path=None):
        if file_path is not None:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        else:
            df = pd.read_excel(self.file_path, sheet_name=sheet_name)
        return df

    def to_excel(self, sheet_list, file_path=None, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11, column_width=17):
        if file_path is None:
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