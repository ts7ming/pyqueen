# PyQueen

![github license](https://img.shields.io/github/license/ts7ming/pyqueen)
[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)
![Language](https://img.shields.io/badge/language-Python-brightgreen)
[![Documentation Status](https://readthedocs.org/projects/pyqueen/badge/?version=latest)](https://pyqueen.readthedocs.io/zh-cn/latest/?badge=latest)
![GitHub Repo stars](https://img.shields.io/github/stars/ts7ming/pyqueen)

PyQueen 是一个简单的数据处理工具箱. 配合 Pandas 使用可以完成简单的ETL作业

## Install
```bash
pip install pyqueen
```

## Doc
- [readthedocs](https://pyqueen.readthedocs.io/zh-cn/latest/)
- 示例: example/*

#### 读写数据库
- dbtype: 可选 mysql,mssql,oracle,clickhouse,sqlite
- 每次操作数据库都会销毁连接, 无需手动close_conn. 如需手动控制连接 添加: `ds.keep_conn()`
  和 `ds.close_conn()`
- 如需切换 db_name 添加: `ds.set_db(db_name)`
- 设置字符集 添加: `ds.set_charset(charset)`. 默认: `utf8mb4`
- 设置 chunksize 添加 `ds.set_chunksize(1000)`. 默认: `10000`

```python
from pyqueen import DataSource

ds = DataSource(host='', username='', password='', port='', db_name='', db_type='')

# 根据sql查询, 返回 pd.DataFrame 对象
df = ds.get_sql(sql='select * from table')

# 返回查询结果的第一个值
v = ds.get_value(sql='select count(1) from table')

# 将 pd.DataFrame对象 写入数据库
### fast_load: 默认False; 仅支持MySQL, 将 pd.DataFrame对象 写入临时csv再快速导入数据库 (如果数据包含特殊字符容易出错, 慎用)
ds.to_db(df=df_to_write, tb_name='')

# 执行sql
ds.exe_sql(sql='delete from table')
```

#### 下载FTP文件

```python
from pyqueen import DataSource

ds = DataSource(host='', username='', password='', port='', db_type='ftp')
ds.download_ftp(local_dir='保存目录', remote_dir='远程目录')
```

#### 写入Excel文件
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

#### 时间处理工具
```python
from pyqueen import TimeKit

# 按当前时间
tk = TimeKit()
# 指定日期, 时间
tk = TimeKit(theday=20200101, thetime=120000)

# 常用属性
tk.today    # 当前日期或初始化指定日期
tk.now    # 当前时间或初始化指定时间
tk.hour    # 当前小时
tk.minute    # 当前分钟
tk.second    # 当前秒
tk.nday_of_week    # 1-7对应周一到周日
tk.week_start    # 本周一日期
tk.lw_start    # 上周开始日期
tk.lw_end    # 上周结束日期
tk.lw2_start    # 上上周开始日期
tk.lw2_end    # 上上周结束日期
tk.month_start    # 本月初
tk.lm_start    # 上月初
tk.lm_end    # 上月末
tk.lm2_start    # 上上月初
tk.lm2_end    # 上上月末

# 时间加减
# flag: 加减单位: years,months,days,hours,minutes,seconds
# value: 加减值
# thetime之前 value 写负值
# thetime之后 value 写正值
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

#### ETL日志
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

#### 发送信息
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
wechat = Dingtalk(access_token='')

# content不为None时,发送文本
# mentioned_list: userid的列表，提醒群中的指定成员(@某个成员)，@all表示提醒所有人
# mentioned_mobile_list: 手机号列表，提醒手机号对应的群成员(@某个成员)，@all表示提醒所有人
wechat.send(content=None, mentioned_list=None, mentioned_mobile_list=None)
```

#### 小工具
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
```