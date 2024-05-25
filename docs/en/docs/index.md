# PyQueen

![](logo.jpg)

![github license](https://img.shields.io/github/license/ts7ming/pyqueen)
[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)
![Language](https://img.shields.io/badge/language-Python-brightgreen)
[![Documentation Status](https://readthedocs.org/projects/pyqueen/badge/?version=latest)](https://pyqueen.readthedocs.io/zh-cn/latest/?badge=latest)
![GitHub Repo stars](https://img.shields.io/github/stars/ts7ming/pyqueen)
![GitHub forks](https://img.shields.io/github/forks/ts7ming/pyqueen)
[![PyPI downloads](https://img.shields.io/pypi/dm/pyqueen.svg)](https://pypistats.org/packages/pyqueen)

PyQueen is a data development toolkit that can build ETL workflows

## Doc
- [Document](https://pyqueen.readthedocs.io/en/latest/)
- [中文文档](https://pyqueen.readthedocs.io/zh-cn/latest/)

## Install

```bash
pip install pyqueen
```

## Data IO

read and write databases and other datasource

#### 参数 Parameters:
- conn_type: mysql,oracle,mssql,clickhouse,pgsql,sqlite,jdbc,redis,excel,ftp,web
- host: option, default:None. conn_type in (mysql, oracle, mssql, clickhouse, pgsql, redis, ftp) 
- username: option, default:None. conn_type in (mysql, oracle, mssql, clickhouse, pgsql, redis, ftp) 
- password: option, default:None. conn_type in (mysql, oracle, mssql, clickhouse, pgsql, redis, ftp) 
- port: option, default:None. conn_type in (mysql, oracle, mssql, clickhouse, pgsql, redis, ftp) 
- db_name: option, default:None. conn_type in (mysql, oracle, mssql, clickhouse, pgsql, redis) 
- file_path: option, default:None. conn_type in (sqlite, excel) 时
- jdbc_url: option, default:None. conn_type in (mysql, oracle, mssql, clickhouse, pgsql, sqlite) 
- cache_dir: option, default:None. conn_type in (web, ) 时
- keep_conn: option, default:False. conn_type in (mysql, oracle, mssql, clickhouse, pgsql, jdbc_url, redis) 
    - use False, auto `ds.create_conn` and `ds.close_conn`
    - use True, auto `ds.create_conn`, and manual `ds.close_conn()`
- charset: option, default:None. auto charset depends on conn_type
- conn_package: option, default:None. auto package depends on conn_type
    - mysql: default: pymysql
    - mssql: default: pymssql
        - option pyodbc
    - oracle: default: cx_oracle
    - clickhouse: default: clickhouse-driver
    - postgresql: default: psycopg2

#### 函数
- read_sql(sql)
- get_sql(sql) same as read_sql
- exe_sql(sql)
- to_db(df, tb_name[, how, fast_load, chunksize])
  - df: pd.DataFrame()对象
  - tb_name: target table name
  - how: option, default append, 
  - fast_load: option, default False; only support MySQL and Clickhouse, export pd.DataFrame to a temp csv then import to db
  - chunksize: option, default 10000
- read_excel(sheet_name[, file_path])
  - sheet_name
  - file_name: option, default None取 self.file_path
- to_excel(sheet_list[, file_path=None, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11, column_width=17])
  - sheet_list: [[df1, 'sheet_name1'], [df2, 'sheet_name2'],]
  - file_path: option, default None取 self.file_path
  - fillna: option, default ''
  - fmt: option, default None
  - font: option, default '微软雅黑'
  - font_color: option, default 'black'
  - font_size: option, default 11
  - column_width: option, default 17
- get_v(key): 
- set_v(key, value): 
- download_dir(local_dir, remote_dir)
  - local_dir: 
  - remote_dir: 
- read_page(url)
  - init DataSource with cache_dir, save page source, read from cache_dir next time
- set_logger([logger])
  - logger: option, default None, use 'file', or customer function
- row_count(table_name):
- get_sql_group(sql, params)
  - sql: sql template
  - params: param list
- pdsql(sql, data)
  - sql: every pd.DataFrame as a table
  - data: [[df_name1, df1],[df_name2, df2],]
- to_images(df[, file_path, col_width, font_size])
  - df: pd.DataFrame
  - file_path: option, default None to a temp file
  - col_width: option, default None auto
  - font_size: option, default None auto
- delete_file(path)
- get_tmp_file()


```python
from pyqueen import DataSource

ds = DataSource(conn_type='mysql', host='', username='', password='', port='', db_name='')

# run sql and return pd.DataFrame
df = ds.get_sql(sql='select * from table')

# return the first value in result
v = ds.get_value(sql='select count(1) from table')

# write a pd.DataFrame to database
### fast_load: default False; only support MySQL and Clickhouse
### write pd.DataFrame to a temporary csv file, then load into database
ds.to_db(df=df_to_write, tb_name='')

# run SQL
ds.exe_sql(sql='delete from table')
sql1 = 'delete from table'
sql2 = 'insert into table select * from table2'
sql3 = 'update table set a=1'
ds.exe_sql(sql=[sql1, sql2, sql3])

# KV databases
ds = DataSource(conn_type='redis', host='', username='', password='', port='', db_name='')
ds.set_v(key='kk', value='vv')
ds.get_v(key='kk')

# pd.DataFrame to image
### file_path: image file path. generate temporary file by default
### col_width: set width for every column
### font_size: font size
path = ds.to_image(df, file_path=None, col_width=None, font_size=None)

# download web pages
### use `cache_dir` will save the web page to `cache_dir`, and reads it from the local next time to avoid repeated requests for services

ds = DataSource(cache_dir='')
page = ds.read_page(url='')

### get text content
from pyqueen import Utils
text = Utils.html2text(page)
```

## Models

```python
from pyqueen import Model

data = df['to_forecast_col']  # alse support list
### forecast_step: numbers need to forecast
### p,d,q: params for arima. auto generate by default
forecast_result = Model.arima(data, forecast_step=10, p=None, d=None, q=None)
```

## Other ETL Function

```python
# SQL on pd.DataFrame (depend on duckdb)
### example python code
df_fact = pd.merge(df_fact, df_dim1, on='d1', how='inner')
df_fact = pd.merge(df_fact, df_dim1, on='d2', how='inner')
df_summary = df_fact.groupby(['d1_name', 'd2_name']).agg({'value': np.sum}).reset_index().rename('value':'sum_value')

### also works
from pyqueen import DataSource

ds = DataSource()
data = {'table1': df_fact, 'table2': df_dim1, 'table3': df_dim2}
sql = '''
  select b.d1_name,c.d2_name,sum(a.value) as sum_value
  from table1 a 
  inner join table2 b on a.d1 = b.d1
  inner join table3 c on a.d2 = c.d2
  group by b.d1_name,c.d2_name
'''
df_summary = ds.pdsql(sql=sql, data=data)
```

## Download FTP files

```python
from pyqueen import DataSource

ds = DataSource(host='', username='', password='', port='', db_type='ftp')
ds.download_ftp(local_dir='', remote_dir='')
```

## Chart

```python
import pandas as pd
from pyqueen import Chart

df = pd.DataFrame()

Chart.line(x=df['x_col'], y=df['y_col'], x_label='', y_label='', img_path='demo.png', show=True)
Chart.bar(x=df['x_col'], y=df['y_col'], x_label='', y_label='', img_path='demo.png', show=True)
Chart.scatter(x=df['x_col'], y=df['y_col'], x_label='', y_label='', img_path='demo.png', show=True)
Chart.bubble(x=df['x_col'], y=df['y_col'], v=df['value_col'], c=df['color'], x_label='', y_label='',img_path='demo.png', show=True)
```

## Save Excel file

- save pd.DataFrame objects to excel
- file_path:  (end with `.xlsx`)
- sheet_list: each DataFrame to a sheet

```python
from pyqueen import DataSource

ds = DataSource()

sheet_list = [
    [df1, 'sheet_name1'],
    [df2, 'sheet_name2']
]
fmt = {
    'col1': '#,##0',
    'col2': '#,##0.0',
    'col3': '0%',
    'col4': '0.00%',
    'col5': 'YYYY-MM-DD'
}
ds.to_excel(file_path='xxx.xlsx', sheet_list=sheet_list, fmt=fmt)
```

## Time Kit

```python
from pyqueen import TimeKit

# curent time
tk = TimeKit()
# set date and time
tk = TimeKit(theday=20200101, thetime=120000)

# Attrs
tk.today
tk.now
tk.hour
tk.minute
tk.second
tk.nday_of_week
tk.week_start
tk.lw_start
tk.lw_end
tk.lw2_start
tk.lw2_end
tk.month_start
tk.lm_start
tk.lm_end
tk.lm2_start
tk.lm2_end

# time delta
# flag: years,months,days,hours,minutes,seconds or 年,月,日,时,分,秒
# value

## by tk.now or tk.today
## if need long datetime: short=False. default True
new_day = tk.delta('days', -30, short=True)

## by specified time
new_day = tk.time_delta('20230101', 'days', -30)

# get days list
day_list = tk.get_day_list(20200101, 20200201)
# get weeks list
week_list = tk.get_week_list(20200101, 20200201)
# get months list
month_list = tk.get_month_list(20200101, 20200901)
# divde period into date list by days
time_list = tk.date_div(20200101, 20200901, 10)
# get num of date in a week
n = tk.get_nday_of_week(20200101)
# date int to string
date_str = tk.int2str(20200101, sep='-')
```

## ETL Log

- use `ds.set_logger(logger)` to open log
    - function `logger`, default: `print`
    - user-defined logger reference `example/etl_with_log.py`
- `etl_log` **key**
    - py_path: python script path
    - func_name: function name
    - start_time: step start time
    - end_time: step end time
    - duration: duration(s)
    - message: (option)from annotation
    - file_path: (option) file path
    - sql_text: (option) sql
    - host: (option)
    - db_type: (option)
    - port: (option)
    - db_name: (option)
    - table_name: (option)

## Send Message

- Email
- Dingtalk
- WeChat

```python
from pyqueen import Email

# init
email = Email(username='', password='', host='', port='')

# text email
email.send_text(subject='', content='', to_user=[], cc_user=None, bcc_user=None, type='plain')

# email with files
email.send_file(subject='', content='', file_path_list=[], to_user=[], cc_user=None, bcc_user=None, type='plain')
```

```python
from pyqueen import Wechat

# init
wechat = Wechat(key='')
wechat.send(content=None, mentioned_list=None, mentioned_mobile_lis=None, file_path=None, img_path=None)
```

```python
from pyqueen import Dingtalk

# init
dingtalk = Dingtalk(access_token='')
dingtalk.send(content=None, mentioned_list=None, mentioned_mobile_list=None)
```

## Tools

```python
from pyqueen import Utils

Utils.zip(from_path='', to_path='')
Utils.unzip(from_path='', to_path='')

Utils.delete_file(path='')

Utils.md5(text='')

Utils.div_list(listTemp=[1, 2, 3], n=2)

Utils.sql2table(sql_text='', kw=None, strip=None)

result = Utils.mult_run(func, args_list=[], max_process=1)

text = Utils.html2text(html)
```

## Command Line

```commandline
usage: pyqueen command args1,args2,...
---
command: 
    #1  sql2table [file_path]
    
    #2  detcode file_path
    
    #3  md5
```