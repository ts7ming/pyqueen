from pyqueen import DataSource, TimeKit

tk = TimeKit()
start_date = tk.int2str(tk.lm_start)
end_date = tk.int2str(tk.today)

ds = DataSource(host='localhost', username='root', password='root', port=3306, db_name='mydb', db_type='mysql')

sql = '''
select * from table where order_date>='{start_date}' and order_date<='{end_date}'
'''.format(start_date=start_date, end_date=end_date)

df = ds.get_sql(sql)

# do something with DataFrame

ds.to_db(df, 'table2')
