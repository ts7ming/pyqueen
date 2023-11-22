import pandas as pd
import numpy as np
import xlsxwriter
import os


class Excel:
    @staticmethod
    def read_excel(path, sheet_name=None):
        df = pd.read_excel(path, sheet_name=sheet_name)
        return df

    @staticmethod
    def to_excel(file_path, sheet_list, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11,
                 column_width=17):
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
