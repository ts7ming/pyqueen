from pyqueen import DataSource, TimeKit


def my_logger(etl_log):
    """
    处理 etl_log. 注意: 不是每个操作都包含以下所有值

    etl_log = {
        'py_path':'',
        'func_name':'',
        'start_time':'',
        'end_time':'',
        'duration':'',
        'message':'',
        'file_path':'',
        'sql_text':'',
        'host':'',
        'db_type':'',
        'port':'',
        'db_name':'',
        'table_name':''
    }
    """
    print(etl_log)


tk = TimeKit()
start_date = tk.int2str(tk.lm_start)
end_date = tk.int2str(tk.today)

ds = DataSource(host='localhost', username='root', password='root', port=3306, db_name='mydb', db_type='mysql')
# ds.set_logger() # 日志仅输出到屏幕
# ds.set_logger('file') # 将日志保存到当前目录下同名.log文本文件
ds.set_logger(my_logger)

sql = '''
select * from table where date>='{start_date}' and date<='{end_date}'
'''.format(start_date=start_date, end_date=end_date)

df = ds.get_sql(sql)

# do something with DataFrame

ds.to_db(df, 'table2')
