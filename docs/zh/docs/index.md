# PyQueen

![](logo.jpg)

![github license](https://img.shields.io/github/license/ts7ming/pyqueen)
[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)
![Language](https://img.shields.io/badge/language-Python-brightgreen)
[![Documentation Status](https://readthedocs.org/projects/pyqueen/badge/?version=latest)](https://pyqueen.readthedocs.io/zh-cn/latest/?badge=latest)
![GitHub Repo stars](https://img.shields.io/github/stars/ts7ming/pyqueen)
![GitHub forks](https://img.shields.io/github/forks/ts7ming/pyqueen)
[![PyPI downloads](https://img.shields.io/pypi/dm/pyqueen.svg)](https://pypistats.org/packages/pyqueen)
[![PyPI Downloads](https://static.pepy.tech/badge/pyqueen)](https://pepy.tech/projects/pyqueen)

PyQueen 是一个数据处理工具箱, 用于构建ETL工作流.

## 文档
- [Document](https://pyqueen.readthedocs.io/en/latest/)
- [中文文档](https://pyqueen.readthedocs.io/zh-cn/latest/)

## 安装

```bash
pip install pyqueen
```

## 数据读写

pyqueen.DataSource: 读写数据库, 文件和其他类型的数据源

#### 参数:
- conn_type: 支持 mysql,oracle,mssql,clickhouse,pgsql,sqlite,jdbc,redis,excel,ftp,web
    - (为兼容1.0版本,暂时可用 db_type 代替)

- username: 可选, 默认:None
    - conn_type 为 (mysql, oracle, mssql, clickhouse, pgsql, redis, ftp) 时

- password: 可选, 默认:None
    - conn_type 为 (mysql, oracle, mssql, clickhouse, pgsql, redis, ftp) 时

- port: 可选, 默认:None
    - conn_type 为 (mysql, oracle, mssql, clickhouse, pgsql, redis, ftp) 时

- db_name: 可选, 默认:None
    - conn_type 为 (mysql, oracle, mssql, clickhouse, pgsql, redis) 时

- file_path: 可选, 默认:None
    - conn_type 为 (sqlite, excel) 时

- jdbc_url: 可选, 默认:None
    - conn_type 为 (mysql, oracle, mssql, clickhouse, pgsql, sqlite) 时
    - 指定 jdbc_url 后优先用url创建连接

- cache_dir: 可选, 默认:None
    - conn_type 为 (web, )

- keep_conn: 可选, 默认:False
    - conn_type 为 (mysql, oracle, mssql, clickhouse, pgsql, jdbc_url, redis) 时
    - 为 False 时, 每次操作数据库都会销毁连接, 无需关注连接池情况
    - 为 True 时, 使用后需手动 `ds.close_conn()`

- charset: 可选, 默认:None. 根据 conn_type 自动使用最常用的字符集
    - 支持指定字符集


#### 函数
- read_sql(sql[, data, engine]): 读取sql
    - 如果 `conn_type` 是数据库, 执行sql结果返回为 pd.DataFrame 对象
    - 如果 `conn_type` 是 excel, 当前excel文件每个sheet映射为一张表, 执行sql结果返回为 pd.DataFrame 对象
    - 如果 `conn_type` 为空, 也可以 传入 data 每个 df 映射为一张表, 执行sql结果返回为 pd.DataFrame 对象
        - data: {'tb_name1': df, 'tb_name2': df2}
        - engine: 默认 sqlite, 可以用 duckdb

- get_sql(sql) 功能和 read_sql 一样, 兼容 1.0.x版本

- exe_sql(sql[, auto_commit]): 执行sql
    - 例如 delete/update/insert语句
    - auto_commit: 默认 False; 用于执行 create database 相关操作

- to_db(df, tb_name[, how, fast_load, chunksize]): 写入数据库
    - df: 待写入 pd.DataFrame() 对象
    - tb_name: 目标表名, 目标库没有的话自动创建
    - how: 可选, 默认 append:追加
    - fast_load: 可选, 默认False; 仅支持MySQL 和 Clickhouse, 将 pd.DataFrame对象写入临时csv再快速导入数据库 (如果数据包含特殊字符容易出错, 慎用)
    - chunksize: 可选, 默认10000

- read_excel([sheet_name, file_path]): 读取excel表
    - sheet_name: 可选, 默认 None, 取所有sheet
    - file_path: 可选, 默认 None, 取 self.file_path, 可传入 file_path 重新指定

- to_excel(sheet_list[, file_path=None, fillna='', fmt=None, font='微软雅黑', font_color='black', font_size=11, column_width=17]): 写入excel表
    - sheet_list: [[df1, 'sheet_name1'], [df2, 'sheet_name2'],]
    - file_path: 可选, 默认 None, 取 self.file_path, 可传入 file_path 重新指定
    - fillna: 可选, 默认 ''
    - fmt: 可选, 默认 None
    - font: 可选, 默认 '微软雅黑'
    - font_color: 可选, 默认 'black'
    - font_size: 可选, 默认 11
    - column_width: 可选, 默认 17

- get_v(key): 键值数据库取值

- set_v(key, value): 键值数据库更新值

- download_dir(local_dir, remote_dir)
    - local_dir: 本地目录
    - remote_dir: 待下载远程目录

- read_page(url)
    - 初始化 DataSource 时指定 cache_dir, 可以缓存页面, 下次访问时直接从缓存读取

- set_logger([logger])
    - logger: 可选, 默认 None, 可使用预置的 'file', 也可以传入自定义函数
    - log_path: 如果 logger = 'file' 可以指定日志文件, 否则自动写入当前目录下
    - log_ds: 如果 logger = 'db' 指定用于保存日志的 DataSource 对象
    - log_tb: 如果 logger = 'db' 指定用于保存日志的表名
    - server_id: 如果 logger = 'db' 指定服务器id, 用于区分不同服务器的日志. 为None自动取 `'[' + username + ']@['+str(host)+']:['+str(port)+']'`



- row_count(table_name): 统计表行数

- get_sql_group(sql, params)
    - sql: sql模板
    - params: 参数列表

- to_images(df[, file_path, col_width, font_size])
    - df: pd.DataFrame
    - file_path: 可选, 默认 None 写入临时路径
    - col_width: 可选, 默认 None 自动确定
    - font_size: 可选, 默认 None 自动确定

- delete_file(path)

- get_tmp_file()

#### 示例

```python
from pyqueen import DataSource

# conn_type 支持: mysql,oracle,mssql,clickhouse,pgsql,sqlite,jdbc,redis,excel,ftp,web
# 为了兼容1.0版本, 目前 db_type 与 conn_type 都可用
ds = DataSource(conn_type='mysql', host='', username='', password='', port='', db_name='')
ds.set_db('new_db')
# 根据sql查询, 返回 pd.DataFrame 对象
df = ds.read_sql(sql='select * from table')

# 返回查询结果的第一个值
v = ds.get_value(sql='select count(1) from table')

# 将 pd.DataFrame对象 写入数据库
### fast_load: 默认False; 仅支持MySQL和Clickhouse, 将 pd.DataFrame对象 写入临时csv再快速导入数据库 (如果数据包含特殊字符容易出错, 慎用)
ds.to_db(df=df_to_write, tb_name='')

# 执行sql
ds.exe_sql(sql='delete from table')
sql1 = 'delete from table'
sql2 = 'insert into table select * from table2'
sql3 = 'update table set a=1'
ds.exe_sql(sql=[sql1, sql2, sql3])


# 键值数据库
ds = DataSource(conn_type='redis', host='', username='', password='', port='', db_name='')
ds.set_v(key='kk', value='vv')
ds.get_v(key='kk')

# pd.DataFrame 转图片
### 可以指定文件路径: file_path. 默认生成临时文件
### 可以用列表为每一列指定宽度 col_width
### 指定字体大小 font_size
path = ds.to_image(df, file_path=None, col_width=None, font_size=None)

# 下载网页文本
### `cache_dir` 的作用是缓存网页html到 `cache_dir`, 下次访问直接从本地加载, 避免频繁请求页面
ds = DataSource(cache_dir='')
page = ds.read_page(url='https://www.github.com')

### 去除html字符, 只保留文本 (保留页面所有文本, 如需精确筛选需要自行解析html)
from pyqueen import Utils
text = Utils.html2text(page)
```

## 常用模型

```python
from pyqueen import Model

data = df['待预测列']  # 也可以是 list形式的数据
### forecast_step: 预测节点数
### p,d,q: 自定义arima模型参数. 为空时自动使用最优模型
forecast_result = Model.arima(data, forecast_step=10, p=None, d=None, q=None)
```
## ETL辅助功能

```python
# 使用SQL语法查询 pd.DataFrame 数据 (功能依赖duckdb包); 可以部分代替 pandas接口 
### 等价python
df_fact = pd.merge(df_fact, df_dim1, on='d1', how='inner')
df_fact = pd.merge(df_fact, df_dim1, on='d2', how='inner')
df_summary = df_fact.groupby(['d1_name', 'd2_name']).agg({'value': np.sum}).reset_index().rename('value':'sum_value')

### 可以用sql实现
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
df_summary = ds.read_sql(sql=sql, data=data)

### 用sql查询excel文件
# excel:
ds = DataSource(conn_type='excel', file_path='myexcel.xlsx')
sql = '''
select * from sheet1 union all select * from sheet2
'''
df_summary = ds.read_sql(sql=sql)

```

## 下载FTP文件

```python
from pyqueen import DataSource

ds = DataSource(host='', username='', password='', port='', db_type='ftp')
ds.download_dir(local_dir='保存目录', remote_dir='远程目录')
```

## 解析帆软SQL

```python
from pyqueen import FineReport

fr = FineReport()
data = fr.extract_sql('fr/file/dir')
data = [
  {'fr_path':'帆软模板(.cpt/.frm)路径','server':'帆软数据连接','fr_dataset':'帆软数据集','sql':'select * from tb'},
  {'fr_path':'帆软模板(.cpt/.frm)路径','server':'帆软数据连接','fr_dataset':'帆软数据集','sql':'select * from tb'},
]
```

## 图表

```python
import pandas as pd
from pyqueen import Chart

df = pd.DataFrame()

# 折线图/柱状图/散点图/气泡图
# img_path 不为None时保存图片, show为False时静默运行不弹出图片窗口
Chart.line(x=df['x_col'], y=df['y_col'], x_label='', y_label='', img_path='demo.png', show=True)
Chart.bar(x=df['x_col'], y=df['y_col'], x_label='', y_label='', img_path='demo.png', show=True)
Chart.scatter(x=df['x_col'], y=df['y_col'], x_label='', y_label='', img_path='demo.png', show=True)
Chart.bubble(x=df['x_col'], y=df['y_col'], v=df['value_col'], c=df['color'], x_label='', y_label='',img_path='demo.png', show=True)
```

## 写入Excel文件

- 将 pd.DataFrame对象 写入Excel文件
- file_path 文件路径 (须以 .xlsx 结尾)
- sheet_list 待写入数据, 二维列表, 每个 pd.DataFrame对象 对应一个 sheet
- fillna='' 空值填充
- fmt=None 字段格式,可以按字段名指定
- font='微软雅黑' 字体
- font_color='black' 字体颜色
- font_size=11 字体大小
- column_width=17 单元格宽度

```python
from pyqueen import DataSource

ds = DataSource(file_path='file_path.xlsx')

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
ds.to_excel(sheet_list=sheet_list, fmt=fmt)
# or 
ds.to_excel(file_path='/new_path/file.xlsx', sheet_list=sheet_list, fmt=fmt)

# sql on Excel
## 将一个Excel作为一个数据库, 每个sheet作为一张表, 通过sql查询
sql = '''
select * from df1 union all select * from df2
'''
df_new = ds.read_sql(sql=sql)
```

## 时间处理工具

```python
from pyqueen import TimeKit

# 按当前时间
tk = TimeKit()
# 指定日期, 时间
tk = TimeKit(theday=20200101, thetime=120000)

# 常用属性
tk.today  # 当前日期或初始化指定日期
tk.now  # 当前时间或初始化指定时间
tk.hour  # 当前小时
tk.minute  # 当前分钟
tk.second  # 当前秒
tk.nday_of_week  # 1-7对应周一到周日
tk.week_start  # 本周一日期
tk.lw_start  # 上周开始日期
tk.lw_end  # 上周结束日期
tk.lw2_start  # 上上周开始日期
tk.lw2_end  # 上上周结束日期
tk.month_start  # 本月初
tk.lm_start  # 上月初
tk.lm_end  # 上月末
tk.lm2_start  # 上上月初
tk.lm2_end  # 上上月末

# 时间加减
# flag: 加减单位: years,months,days,hours,minutes,seconds 或者 年,月,日,时,分,秒
# value: 加减值
# thetime之前 value 写负值
# thetime之后 value 写正值

## 按当前时间
## 如果需要长时期时间 (14位int) 指定 short=False. 默认为 True
new_day = tk.delta('days', -30)

## 按任意日期
new_day = tk.time_delta('20230101', 'days', -30)

# 获取日期列表
day_list = tk.get_day_list(20200101, 20200201)
# 获取自然周列表
week_list = tk.get_week_list(20200101, 20200201)
# 获取自然月列表
month_list = tk.get_month_list(20200101, 20200901)
# 按天数拆分日期为列表
time_list = tk.date_div(20200101, 20200901, 10)
# 查询任意日期是星期几
n = tk.get_nday_of_week(20200101)
# 数值型日期转字符串
date_str = tk.int2str(20200101, sep='-')
```

## ETL日志

- 记录所有 `DataSource` 类函数的调用过程和相应参数
- 如需启用日志, 添加: `ds.set_logger(logger)`
- 其中 `logger` 为日志处理函数, 默认为: `print`
- 自定义 `logger` 参考 `example/etl_with_log.py`
- `etl_log` 所有 **key**
    - py_path: 调用脚本路径
    - func_name: 调用函数名
    - start_time: 过程开始时间
    - end_time: 过程结束时间
    - duration: 过程耗时(秒)
    - message: (如有) 备注信息
    - file_path: (如有) 文件路径
    - sql_text: (如有) sql
    - host: (如有) 服务器地址
    - db_type: (如有) 数据库类型
    - port: (如有) 端口
    - db_name: (如有) 数据库名
    - table_name: (如有) 表名

## 发送信息

- 邮件
- 钉钉
- 企业微信

```python
from pyqueen import Email

# 初始化
email = Email(username='', password='', host='', port='')

# 发送文本邮件
# subject: 邮件主题，content: 邮件内容，to_user: 收件人，cc_user: 抄送人，bcc_user: 密抄人
# type: 文本或html格式，默认文本格式
email.send_text(subject='', content='', to_user=[], cc_user=None, bcc_user=None, type='plain')

# 发送附件邮件
# subject: 邮件主题，content: 邮件内容，to_user: 收件人，cc_user: 抄送人，bcc_user: 密抄人
# type: 文本或html格式，默认文本格式，file_path_list: 附件路径列表
email.send_file(subject='', content='', file_path_list=[], to_user=[], cc_user=None, bcc_user=None, type='plain')
```

```python
from pyqueen import Wechat

# 初始化
wechat = Wechat(key='')

# content不为None时,发送文本
# mentioned_list: userid的列表，提醒群中的指定成员(@某个成员)，@all表示提醒所有人
# mentioned_mobile_list: 手机号列表，提醒手机号对应的群成员(@某个成员)，@all表示提醒所有人
# file_path不为None时,发送文件
# img_path不为None时,发送图片
wechat.send(content=None, mentioned_list=None, mentioned_mobile_lis=None, file_path=None, img_path=None)
```

```python
from pyqueen import Dingtalk

# 初始化
dingtalk = Dingtalk(access_token='')

# content不为None时,发送文本
# mentioned_list: userid的列表，提醒群中的指定成员(@某个成员)，@all表示提醒所有人
# mentioned_mobile_list: 手机号列表，提醒手机号对应的群成员(@某个成员)，@all表示提醒所有人
dingtalk.send(content=None, mentioned_list=None, mentioned_mobile_list=None)
```

## 小工具

```python
from pyqueen import Utils

# 压缩/解压缩
Utils.zip(from_path='', to_path='')
Utils.unzip(from_path='', to_path='')
# 删除文件
# 删除文件夹/子文件夹/文件
Utils.delete_file(path='')
# 计算md5值
Utils.md5(text='')
# 列表按n个一组拆分
Utils.div_list(listTemp=[1, 2, 3], n=2)
# 用正则从sql里提取用到的表
### kw: (可选)指定匹配关键词
### strip: (可选)指定需要清除的字符
Utils.sql2table(sql_text='', kw=None, strip=None)
# 多进程执行
### func: 待执行函数
### args_list: 每个子任务的参数
### max_process = 1: 最大进程数, 默认为 1
### 以list返回每个子进程执行结果, 和 args_list 顺序一致
result = Utils.mult_run(func, args_list=[], max_process=1)
# html转文本
### 去除html字符, 只保留文本
text = Utils.html2text(html)
```

## 命令行

```commandline
用法: pyqueen command args1,args2,...
---
command: 
    #1  sql2table [file_path] 从sql解析用到的表(通过正则解析, 有误差) (不带参数时读取剪切板)
    
    #2  detcode file_path: 检测文件编码
    
    #3  md5 基于剪切板文本生成md5
```

