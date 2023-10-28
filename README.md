# PyQueen

## 安装
```bash
# 安装 & 卸载
$ pip3 install pyqueen --upgrade
$ pip3 uninstall pyqueen
```

## DataSource



## 模块
### etl-基础etl功能
#### 数据源管理: DataSource
```python
from pyqueen import DataSource
# ds实例化
ds = DataSource(host,username,password,port,db_name,db_type)
# db_type: MySQL(default),Oracle,MSSQL, clickhouse, SQLite

# 根据sql查询, 返回 pd.DataFrame对象
df = ds.get_sql(sql_text)

# 将 pd.DataFrame对象 写入数据库
ds.to_db(df_to_write_in, table_name, how='append', fast_load=False)
# fast_load: 仅支持MySQL, 将 pd.DataFrame对象 写入临时csv再快速导入数据库 (如果数据包含特殊字符容易出错, 慎用)

# 将 pd.DataFrame对象 写入Excel文件
sheet_list = [
	[df1, 'sheet_name1'],
	[df2,'sheet_name2']
]
fmt={
	'col1':'#,##0',
	'col2':'#,##0.0',
	'col3':'0%',
	'col4':'0.00%',
	'col5':'YYYY-MM-DD'
}
ds.to_excel(file_path, sheet_list, fmt=fmt)
# file_path 文件路径 (须以 .xlsx 结尾)
# sheet_list 待写入数据, 二维列表, 每个 pd.DataFrame对象 对应一个 sheet
# fillna='' 空值填充
# fmt=None 字段格式,可以按字段名指定
# font='微软雅黑' 字体
# font_color='black' 字体颜色
# font_size=11 字体大小
# column_width=17 单元格宽度

# 执行sql
ds.exe_sql(sql_text)
```

#### 时间工具箱: TimeKit

```python
from pyqueen import TimeKit

# 初始化
tk = TimeKit()
# 可指定日期, 时间
tk = TimeKit(the_day=20200101,the_time=120000)

# 属性
tk.today : 当前日期或初始化指定日期
tk.now : 当前时间或初始化指定时间
tk.hour: 当前小时
tk.minute: 当前分钟
tk.second: 当前秒
tk.nday_of_week : 1-7对应周一到周日
tk.week_start : 本周一日期
tk.lw_start : 上周开始日期
tk.lw_end : 上周结束日期
tk.lw2_start : 上上周开始日期
tk.lw2_end : 上上周结束日期
tk.month_start : 本月初
tk.lm_start : 上月初
tk.lm_end : 上月末
tk.lm2_start : 上上月初
tk.lm2_end : 上上月末

# 时间加减
# flag: 加减单位: years,months,days,hours,minutes,seconds
# value: 加减值
# thetime之前 value 写负值
# thetime之后 value 写正值

new_day = tk.time_delta(today, 'days', -30)

# 获取日期列表
day_list = tk.get_day_list(20200101, 20200201)
# 获取自然周列表
week_list = tk.get_week_list(20200101, 20200201)
# 获取自然月列表
month_list = tk.get_month_list(20200101, 20200901)
# 按天数拆分日期为列表
time_list = tk.date_div(start, end, num)
# 查询任意日期是星期几
n = get_nday_of_week(day)
# 数值型日期转字符串
date_str = tk.int2str(date_int, sep='-')
```

### service-通用服务
- 通用服务

#### service.Email
```python
from pyqueen.service import Email

# 初始化
email = Email(username, password, host, port)

# 发送文本邮件
# subject: 邮件主题，content: 邮件内容，to_user: 收件人，cc_user: 抄送人，bcc_user: 密抄人
# type: 文本或html格式，默认文本格式
email.send_text(subject, content, to_user, cc_user=None, bcc_user=None, type='plain')

# 发送附件邮件
# subject: 邮件主题，content: 邮件内容，to_user: 收件人，cc_user: 抄送人，bcc_user: 密抄人
# type: 文本或html格式，默认文本格式，file_path_list: 附件路径列表
email.send_file(subject, content, file_path_list, to_user, cc_user,bcc_user,type='plain')
```

#### service.Wechat
```python
from pyqueen.service import Wechat

# 初始化
wechat = Wechat(key)

# 发送文本类型
# content: 文本内容，最长不超过2048个字节，必须是utf8编码
# mentioned_list: userid的列表，提醒群中的指定成员(@某个成员)，@all表示提醒所有人
# mentioned_mobile_list: 手机号列表，提醒手机号对应的群成员(@某个成员)，@all表示提醒所有人
wechat.send_text( content, mentioned_list, mentioned_mobile_lis)

# 发送markdown类型
# content: markdown内容，最长不超过4096个字节，必须是utf8编码
wechat.send_markdown(content)

# 发送图片类型
# base64: 图片内容的base64编码，md5: 图片内容（base64编码前）的md5值
wechat.send_image(base64, md5)

# 发送文件类型
# file_path: 文件路径
wechat.send_file(file_path)
```

### utils-通用开发工具

#### utils.Ftp

```python
# ftp工具
from pyqueen.utils import Ftp
ftp = Ftp(host, port, username, password)

# 创建ftp连接
ftp.create_connect()

# ftp上传
# remote_path: 远程服务器路径，local_path: 本地路径
ftp.upload_file(remote_path, local_path)

# ftp下载
# remote_path: 远程服务器路径，local_path: 本地路径
ftp.download_file(remote_path, local_path)

# ftp进入指定目录
# remote_path: 远程服务器路径
ftp.cd_remote_path(remote_path, local_path)

# ftp获取当前目录下所有文件
# remote_path: 远程服务器路径
# return: 当前目录下所有文件（列表格式）
path_list = ftp.loop_remote_list(remote_path, local_path)

# ftp移动文件
# from_path: 原始路径，to_path: 最终路径
ftp.move_remote_file(remote_path, local_path)
```

#### utils.Zip
```python
from pyqueen.utils import Zip

zip = Zip(from_path, to_path)

# 解压
zip.unzip()

# 压缩
zip.pack()
```
