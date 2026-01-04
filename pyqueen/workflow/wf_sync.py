import json
import os
import re
import subprocess
import sys
from ..utility.time_kit import record_time


class WfSync:
    def __init__(self, sync_type, from_ds, from_sql, to_ds, to_table, to_columns, before_write, after_write, param_ds, param_sql, datax_path=None):
        self.sync_type = sync_type
        self.from_ds = from_ds
        self.from_sql = from_sql
        self.to_ds = to_ds
        self.to_table = to_table
        self.to_columns = to_columns
        self.before_write = before_write
        self.after_write = after_write
        self.param_ds = param_ds
        self.param_sql = param_sql
        self.datax_path = datax_path
        self.log = {}

    @record_time()
    def get_param(self):
        if self.param_ds is None:
            return None
        if self.param_sql is None:
            raise Exception('取数sql为空')
        df = self.param_ds.read_sql(self.param_sql)
        if df.shape[0] == 0:
            raise Exception('取数结果为空')
        return df.head(1).to_dict('records')

    @record_time()
    def read_data(self, sql_params):
        if self.from_ds is None:
            raise Exception('数据源为空')
        if self.from_sql is None:
            raise Exception('取数sql为空')
        try:
            real_sql = self.from_sql.format(**sql_params)
            print(real_sql)
        except Exception as e:
            raise Exception('取数sql格式化失败' + str(e))
        df = self.from_ds.read_sql(real_sql)
        return df, real_sql

    @record_time()
    def write_data(self, df):
        if self.to_ds is None:
            raise Exception('目标数据源为空')
        if self.to_table is None:
            raise Exception('目标表名为空')
        if self.to_columns is None:
            self.to_columns = df.columns
        if self.before_write is not None:
            self.before_write()
        df = df[self.to_columns]
        self.to_ds.to_db(df, self.to_table)
        return 0

    @record_time()
    def run_before_write(self, sql_params):
        real_sql = ''
        if self.before_write is not None:
            if sql_params is None:
                real_sql = self.before_write
            else:
                real_sql = self.before_write.format(**sql_params)
            self.to_ds.exe_sql(real_sql)
        return real_sql

    @record_time()
    def run_after_write(self, sql_params):
        real_sql = ''
        if self.after_write is not None:
            if sql_params is None:
                real_sql = self.after_write
            else:
                real_sql = self.after_write.format(**sql_params)
            self.to_ds.exe_sql(real_sql)
        return real_sql

    def get_datax_reader(self, sql_params):
        if self.from_ds is None:
            raise Exception('数据源为空')
        if self.from_sql is None:
            raise Exception('取数sql为空')
        try:
            real_sql = self.from_sql.format(**sql_params)
            print(real_sql)
        except Exception as e:
            raise Exception('取数sql格式化失败' + str(e))
        host = self.from_ds.host
        port = str(self.from_ds.port)
        db_name = self.from_ds.db_name
        if self.from_ds.conn_type == 'oracle':
            reader = 'oraclereader'
            url = f"jdbc:oracle:thin:@{host}:{port}:{db_name}"
        elif self.from_ds.conn_type == 'mssql':
            reader = 'sqlserverreader'
            url = f"jdbc:sqlserver://{host}:{port};DatabaseName={db_name}"
        elif self.from_ds.conn_type == 'mysql':
            reader = 'mysqlreader'
            url = f"jdbc:mysql://{host}:{port}/{db_name}"
        else:
            raise Exception('未知类型')
        url = url.format(host=self.from_ds.host, port=self.from_ds.port, db_name=self.from_ds.db_name)
        cfg = {
            "name": reader,
            "parameter": {
                "username": self.from_ds.username,
                "password": self.from_ds.password,
                "connection": [{"querySql": [real_sql], "jdbcUrl": [url]}]
            }
        }
        return cfg

    def get_datax_writer(self, sql_params):
        if self.before_write is None:
            before_write = ''
        elif sql_params is None:
            before_write = self.before_write
        else:
            before_write = self.before_write.format(**sql_params)

        if self.after_write is None:
            after_write = ''
        elif sql_params is None:
            after_write = self.after_write
        else:
            after_write = self.after_write.format(**sql_params)

        if self.to_ds.conn_type == 'oracle':
            writer = 'oraclewriter'
            url = "jdbc:oracle:thin:@{host}:{port}:{db_name}"
        elif self.to_ds.conn_type == 'mssql':
            writer = 'sqlserverwriter'
            url = "jdbc:sqlserver://{host}:{port};DatabaseName={db_name}"
        elif self.to_ds.conn_type == 'mysql':
            writer = 'mysqlwriter'
            url = "jdbc:mysql://{host}:{port}/{db_name}?useUnicode=true&characterEncoding=utf8"
        elif self.to_ds.conn_type == 'doris':
            writer = "doriswriter"
            url = "jdbc:mysql://{host}:{port}/{db_name}?useUnicode=true&characterEncoding=utf8"
        else:
            raise Exception('未知类型')
        url = url.format(host=self.to_ds.host, port=str(self.to_ds.port), db_name=str(self.to_ds.db_name))
        connection = [{"jdbcUrl": url, "table": [self.to_table]}]
        parameter = {
            "username": self.to_ds.username,
            "password": self.to_ds.password,
            "column": self.to_columns,
            "preSql": [before_write],
            "postSql": [after_write],
            "connection": connection
        }
        if self.to_ds.conn_type == 'doris':
            parameter = {
                "username": self.to_ds.username,
                "password": self.to_ds.password,
                "column": self.to_columns,
                "preSql": [before_write],
                "connection": [{"jdbcUrl": url, "table": [self.to_table], "selectedDatabase": self.to_ds.db_name}],
                "loadUrl": [self.to_ds.host + ':' + self.to_ds.user_vars['be_port']],
            }
        cfg = {
            "name": writer,
            "parameter": parameter
        }
        return cfg

    def sync_with_pandas(self):
        sql_params, p_msg = self.get_param()
        (df, r_sql), r_msg = self.read_data(sql_params)
        bw_real_sql, bw_msg = self.run_before_write(sql_params)
        tmp, w_msg = self.write_data(df)
        aw_real_sql, aw_msg = self.run_after_write(sql_params)

        if df is None or df.empty:
            read_count = 0
        else:
            read_count = df.shape[0]

        msg = {
            'build': {
                'sql_text': self.param_sql,
                'params': sql_params,
                'start_time': p_msg['start_time'],
                'end_time': p_msg['end_time'],
                'duration': p_msg['duration']
            },
            'read': {
                'sql_text': r_sql,
                'read_rows': read_count,
                'start_time': r_msg['start_time'],
                'end_time': r_msg['end_time'],
                'duration': r_msg['duration']
            },
            'bw': {
                'sql_text': bw_real_sql,
                'start_time': bw_msg['start_time'],
                'duration': bw_msg['duration']
            },
            'write': {
                'start_time': w_msg['start_time'],
                'end_time': w_msg['end_time'],
                'duration': w_msg['duration']
            },
            'aw': {
                'sql': aw_real_sql,
                'start_time': aw_msg['start_time'],
                'end_time': aw_msg['end_time'],
                'duration': aw_msg['duration']
            }
        }
        return msg

    @record_time()
    def exe_datax(self, tmp_json_path):
        read_pattern = re.compile(r'读出记录总数\s+:\s+(\d+)', re.IGNORECASE)
        process = subprocess.Popen(
            [sys.executable, self.datax_path, tmp_json_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            encoding='utf-8'
        )
        read_rows = -1
        for line in process.stdout:
            read_match = read_pattern.search(line)
            if read_match:
                read_rows = int(read_match.group(1))
        return_code = process.wait()
        if return_code == 0:
            try:
                os.remove(tmp_json_path)
            except:
                print(f"{tmp_json_path} 删除失败")
        else:
            raise Exception(f"datax 执行失败, 错误信息: {process.stderr}")
        return read_rows

    def sync_with_datax(self):
        sql_params, p_msg = self.get_param()
        reader = self.get_datax_reader(sql_params)
        writer = self.get_datax_writer(sql_params)

        datax_config = {"job": {
            "setting": {"speed": {"channel": 1}, "errorLimit": {"record": 0, "percentage": 0.02}},
            "content": [{"reader": reader, "writer": writer}]
        }}
        tmp_json_path = self.from_ds.get_tmp_file()
        with open(tmp_json_path, 'w') as f:
            json.dump(datax_config, f, indent=4)

        read_rows, dx_msg = self.exe_datax(tmp_json_path)
        msg = {
            'datax': {
                'start_time': dx_msg['start_time'],
                'end_time': dx_msg['end_time'],
                'duration': dx_msg['duration'],
                'read_rows': read_rows
            }
        }
        return msg

    def sync(self):
        if self.sync_type == 'pd':
            msg = self.sync_with_pandas()
        elif self.sync_type == 'datax':
            msg = self.sync_with_datax()
        else:
            raise Exception("未知的同步方式")
        return msg
