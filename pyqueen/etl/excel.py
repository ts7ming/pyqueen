
import xlsxwriter
import os
import sys
import numpy as np
import pandas as pd

class Excel:
    # Excel 相关功能
    def __init__(self, work_path=None):
        if work_path is None:
            work_path=str(os.getcwd()).replace('\\', '/') +'/'
        self.work_path = work_path
        self.check_path(work_path)
    @staticmethod
    def delete_file(path):
        try:
            ls = os.listdir(path)
            for i in ls:
                c_path = os.path.join(path, i)
                try:
                    os.remove(c_path)
                except:
                    print(c_path)
        except:
            pass

    def to_excel(self, file_name, sheet_list,fillna='', fmt=None,font='微软雅黑',font_color='black',font_size=11,column_width=17):

        '''
        **DataFrame对象写入Excel文件**
        :param file_name: str 文件名
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
        
        wb = xlsxwriter.Workbook(self.work_path + file_name + '.xlsx')
        fmt_default= wb.add_format()
        fmt_default.set_font_name(font)
        fmt_default.set_font_color(font_color)
        fmt_default.set_font_size(font_size)

        style={
            'align': 'center',
            'bold': True
        }
        fmt_head= wb.add_format(style)
        fmt_head.set_font_name(font)
        fmt_head.set_font_color(font_color)
        fmt_head.set_font_size(font_size)

        if fmt is None:
            fmt={}
        for df, sheet_name in sheet_list:
            df = df.fillna(fillna)
            df.replace(-np.inf, fillna, inplace=True)
            df.replace(np.inf, fillna, inplace=True)
            ws = wb._add_sheet(sheet_name)
            head = df.columns.to_list()
            data = df.values.tolist()

            row_num,col_num=df.shape
            for col_name,col_index in zip(head,range(col_num)):
                if col_name in fmt.keys():
                    col_format=None
                    col_format = wb.add_format({'num_format': fmt[col_name]}) 
                    col_format.set_font_name(font)
                    col_format.set_font_color(font_color)
                    col_format.set_font_size(font_size)
                else:
                    col_format=fmt_default
                ws.set_column(col_index, col_index, column_width)
                ws.write(0, col_index, str(col_name), fmt_head)
                for row_index in range(row_num):
                    value=data[row_index][col_index]
                    ws.write(row_index+1, col_index, value, col_format)
        wb.close()
        return self.work_path + file_name + '.xlsx'

    def to_excel_with_index(self, file_name, sheet_list, fillna='', column_format=None, decimal=0,text_wrap=0,index_name='目录'):

        '''
        **DataFrame对象写入Excel文件**

        :param file_name: str 文件名
        :param sheet_list: list [[dataframe,sheet_name],[dataframe2,sheet_name2]]
        :param fillna: str 填充NA 值 默认为空
        :param column_format: dict 字段名 格式
        :param decimal: int 保留小数位数
        :return:
        '''
        wb = xlsxwriter.Workbook(self.work_path + file_name + '.xlsx')
        format_conf = {}
        if column_format is None:
            column_format = {}

        format_head = wb.add_format({'text_wrap':text_wrap})  # 格式信息
        format = wb.add_format()  # 格式信息
        format.set_font_name(u'微软雅黑')
        format.set_font_color('black')
        format.set_font_size(11)

        format_head.set_font_name(u'微软雅黑')
        format_head.set_font_color('black')
        format_head.set_font_size(11)

        format_link = wb.add_format({'text_wrap':text_wrap})  # 格式信息
        format = wb.add_format()  # 格式信息
        format.set_font_name(u'微软雅黑')
        format.set_font_color('black')
        format.set_font_size(11)

        format_link.set_font_name(u'微软雅黑')
        format_link.set_font_color('black')
        format_link.set_font_size(11)
        format_link.set_underline()


        format_date = wb.add_format({'num_format': 'YYYY-MM-DD'})  # 格式信息
        if decimal == 0:
            format_num = wb.add_format({'num_format': '#,##0'})  # 格式信息
            format_percent = wb.add_format({'num_format': '0%'})  # 格式信息
        elif decimal == 1:
            format_num = wb.add_format({'num_format': '#,##0.0'})  # 格式信息
            format_percent = wb.add_format({'num_format': '0.0%'})  # 格式信息
        elif decimal == 2:
            format_num = wb.add_format({'num_format': '#,##0.00'})  # 格式信息
            format_percent = wb.add_format({'num_format': '0.00%'})  # 格式信息
        else:
            format_num = wb.add_format({'num_format': '#,##0'})  # 格式信息
            format_percent = wb.add_format({'num_format': '0%'})  # 格式信息

        format_percent.set_font_name(u'微软雅黑')
        format_percent.set_font_color('black')
        format_percent.set_font_size(11)

        format_num.set_font_name(u'微软雅黑')
        format_num.set_font_color('black')
        format_num.set_font_size(11)

        format_date.set_font_name(u'微软雅黑')
        format_date.set_font_color('black')
        format_date.set_font_size(11)

        format_conf['number'] = format_num
        format_conf['percent'] = format_percent
        format_conf['date'] = format_date



        # =HYPERLINK("#'客户整体概览'!A1","客户整体概览")
        # =HYPERLINK("#'目录'!A1","返回目录")
        index_count=1
        ws = wb._add_sheet(index_name)
        sht_name=[x[1] for x in sheet_list]
        ws.set_column(0, 1, 26)

        ws.write(0, 0, '序号', format_head)
        ws.write(0, 1, '表名称', format_link)


        formula='''=HYPERLINK("#'%s'!A1","%s")'''
        formula_sub='''=HYPERLINK("#'%s'!A1","返回目录")''' % index_name
        for sname in sht_name:
            ws.write(index_count, 0, str(index_count), format_link)
            ws.write_formula(index_count, 1, formula % (sname,sname),cell_format=format_link)
            index_count+=1

        column_format_dict = {}
        for df, sheet_name in sheet_list:
            df = df.fillna(fillna)
            df.replace(-np.inf, fillna, inplace=True)
            ws = wb._add_sheet(sheet_name)
            head = df.columns.to_list()
            for j in range(len(head)):
                ws.set_column(j, j, 17)
                if j==0:
                    ws.write_formula(0, 0, formula_sub,cell_format=format_link)
                else:
                    ws.write(0, j, str(head[j]), format_head)
                if str(head[j]) in column_format.keys():
                    column_format_dict[j] = format_conf[column_format[str(head[j])]]
                else:
                    column_format_dict[j] = format
            data = df.values.tolist()
            i = 1
            for row in data:
                j = 0
                for value in row:
                    formats = column_format_dict[j]
                    try:
                        ws.write(i, j, value, formats)
                    except:
                        ws.write(i, j, str(value), formats)
                    j = j + 1
                i = i + 1
        wb.close()
        return self.work_path + file_name + '.xlsx'

    def ranges2excel(self,file_name,range_list,blank_rows=5,fillna='',start_row=1,start_column=1):
        '''
        range :
        :param file_name:
        :param range_list:
        :param blank_rows:
        :return:
        '''
        start_column=start_column-1
        wb = xlsxwriter.Workbook(self.work_path + file_name + '.xlsx')

        format = wb.add_format()  # 格式信息
        format.set_font_name(u'微软雅黑')
        format.set_font_color('black')
        format.set_font_size(11)

        for sheet_name,df_list in range_list:
            ws = wb._add_sheet(sheet_name)
            i = start_row-1
            for title,df in df_list:
                head = df.columns.to_list()
                df = df.fillna(fillna)
                df.replace(-np.inf, fillna, inplace=True)
                ws.write(i,start_column,title,format)
                i=i+1
                for j in range(len(head)):
                    ws.set_column(i, j+start_column, 17)
                    ws.write(i, j+start_column, str(head[j]), format)
                data = df.values.tolist()
                i=i+1
                for row in data:
                    j = start_column-1
                    for value in row:
                        ws.set_column(i, j + start_column, 17)
                        ws.write(i, j + start_column, str(value), format)
                        j=j+1
                    i=i+1
                i=i+blank_rows
        wb.close()
        return self.work_path + file_name + '.xlsx'
    
    @staticmethod
    def check_path(work_path):
        if os.path.exists(work_path) is False:
            os.makedirs(work_path)