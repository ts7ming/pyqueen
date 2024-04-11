# PyQueen

![](logo.jpg)

![github license](https://img.shields.io/github/license/ts7ming/pyqueen)
[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)
![Language](https://img.shields.io/badge/language-Python-brightgreen)
[![Documentation Status](https://readthedocs.org/projects/pyqueen/badge/?version=latest)](https://pyqueen.readthedocs.io/zh-cn/latest/?badge=latest)
![GitHub Repo stars](https://img.shields.io/github/stars/ts7ming/pyqueen)
[![PyPI downloads](https://img.shields.io/pypi/dm/pyqueen.svg)](https://pypistats.org/packages/pyqueen)

PyQueen is a data development toolkit that can build ETL workflows
[中文文档](README_CN.MD)

## Doc
- [readthedocs](https://pyqueen.readthedocs.io/en/latest/)
- [中文版](README_CN.md)

## Install

```bash
pip install pyqueen
```

#### Databases and IO

- dbtype: mysql,mssql,oracle,clickhouse,sqlite
- connection will be destroyed after operation. no need to pay attention to the connection pool
  - if you need keep connection, use `ds.keep_conn()` and `ds.close_conn()`
- use `ds.set_db(db_name)` to change database
- use `ds.set_charset(charset)` to change charset. default `utf8mb4`
- use `ds.set_chunksize(1000)` to change chunksize. default `10000`
- required packages
    - mysql: `pip install pymysql`
    - mssql: `pip install pymssql`
        - or `pip install pyodbc` with `ds.set_package('pyodbc')`
    - oracle: `pip install cx_oracle`
    - clickhouse: `pip install clickhouse-driver`

```python
from pyqueen import DataSource

ds = DataSource(host='', username='', password='', port='', db_name='', db_type='')

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

# pd.DataFrame to image
### file_path: image file path. generate temporary file by default
### col_width: set width for every column
### font_size: font size
path = ds.to_image(df, file_path=None, col_width=None, font_size=None)

# download web pages
### use `set_cache_dir` will save the web page to `cache_dir`, and reads it from the local next time to avoid repeated requests for services

ds.set_cache_dir(cache_dir=None)
page = ds.get_web(url='')

### get text content
from pyqueen import Utils
text = Utils.html2text(html)
```

#### Models

```python
from pyqueen import Model

data = df['to_forecast_col']  # alse support list
### forecast_step: numbers need to forecast
### p,d,q: params for arima. auto generate by default
forecast_result = Model.arima(data, forecast_step=10, p=None, d=None, q=None)
```
#### Other ETL Function 

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

#### Download FTP files

```python
from pyqueen import DataSource

ds = DataSource(host='', username='', password='', port='', db_type='ftp')
ds.download_ftp(local_dir='', remote_dir='')
```

#### Save Excel file

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

#### Time Kit

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
# flag: years,months,days,hours,minutes,seconds
# value
new_day = tk.time_delta('20230101', 'days', -30)
new_day = tk.time_delta('20230101', 'days', 30)

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

#### ETL Log

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

#### 发送信息

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

#### Tools

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

#### Command Line

```commandline
usage: pyqueen command args1,args2,...
---
command: 
    #1  sql2table [file_path]
    
    #2  detcode file_path
    
    #3  md5
```

