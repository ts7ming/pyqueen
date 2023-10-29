from pyqueen import DataSource, TimeKit


def save_log(etl_log):
    info = '\n------------------------------------\n'
    for k, v in etl_log.items():
        info += '    '+str(k) + ': ' + str(v)+'\n'
    info += '\n------------------------------------\n'
    with open('log.txt', 'a+', encoding='utf-8') as f:
        f.write(info)


tk = TimeKit()
start_date = tk.int2str(tk.lm_start)
end_date = tk.int2str(tk.today)

ds = DataSource(host='localhost', username='root', password='root', port=3306, db_name='mydb', db_type='mysql')
ds.set_logger(save_log)

sql = '''
select * from table where order_date>='{start_date}' and order_date<='{end_date}'
'''.format(start_date=start_date, end_date=end_date)

df = ds.get_sql(sql)

# do something with DataFrame

ds.to_db(df, 'table2')
