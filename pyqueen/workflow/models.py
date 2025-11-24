from pyqueen import DataSource, TimeKit, Dingtalk
import time
from config import (
    ds_cfg,
    DATABASES,
    ROBOTS,
    EXECUTOR,
    T_JOB,
    T_JOB_LOG,
    T_CHECK,
    T_SYNC,
    T_PY,
    T_FLINK,
    T_SQL,
    T_MESSAGE,
    T_RPT_PUB,
    T_USER_REQUEST
)

ds = ds_cfg

class Repo:
    @staticmethod
    def get_ds(server_id):
        r_ds = DataSource(**DATABASES[str(server_id)])
        r_ds.auto_server_id = str(server_id)
        return r_ds

    @staticmethod
    def get_datax_reader(server_id, db_name, sql):
        server_cfg = DATABASES[str(server_id)]
        if server_cfg['conn_type'] == 'oracle':
            reader = 'oraclereader'
            url = "jdbc:oracle:thin:@{host}:{port}:{db_name}"
        elif server_cfg['conn_type'] == 'mssql':
            reader = 'sqlserverreader'
            url = "jdbc:sqlserver://{host}:{port};DatabaseName={db_name}"
        elif server_cfg['conn_type'] == 'mysql':
            reader = 'mysqlreader'
            url = "jdbc:mysql://{host}:{port}/{db_name}"
        else:
            raise Exception('未知类型')
        url = url.format(host=server_cfg['host'], port=server_cfg['port'], db_name=db_name)
        cfg = {
            "name": reader,
            "parameter": {
                "username": server_cfg['username'],
                "password": server_cfg['password'],
                "connection": [{"querySql": [sql], "jdbcUrl": [url]}]
            }
        }
        return cfg

    @staticmethod
    def get_datax_writer(server_id, before_write, db_name, table_name, columns=["*"]):
        server_cfg = DATABASES[str(server_id)]
        port = str(server_cfg['port'])
        if server_cfg['conn_type'] == 'oracle':
            writer = 'oraclewriter'
            url = "jdbc:oracle:thin:@{host}:{port}:{db_name}"
        elif server_cfg['conn_type'] == 'mssql':
            writer = 'sqlserverwriter'
            url = "jdbc:sqlserver://{host}:{port};DatabaseName={db_name}"
        elif server_cfg['conn_type'] == 'mysql':
            writer = 'mysqlwriter'
            url = "jdbc:mysql://{host}:{port}/{db_name}?useUnicode=true&characterEncoding=utf8"
        elif server_cfg['conn_type'] == 'doris':
            writer = 'doriswriter'
            url = "jdbc:mysql://{host}:{port}/{db_name}?useUnicode=true&characterEncoding=utf8"
            port = str(server_cfg['port']).split(',')[0]
            port_be = str(server_cfg['port']).split(',')[1]
        else:
            raise Exception('未知类型')
        url = url.format(host=server_cfg['host'], port=port, db_name=db_name)
        connection = [{"jdbcUrl": url, "table": [table_name]}]
        parameter = {
            "username": server_cfg['username'],
            "password": server_cfg['password'],
            "column": columns,
            "preSql": [before_write],
            "connection": connection
        }
        if server_cfg['conn_type'] == 'doris':
            parameter = {
                "username": server_cfg['username'],
                "password": server_cfg['password'],
                "column": columns,
                "preSql": [before_write],
                "connection": [{"jdbcUrl": url, "table": [table_name],"selectedDatabase": db_name}],
                "loadUrl": [server_cfg['host']+':'+port_be],
            }
        cfg = {
            "name": writer,
            "parameter": parameter
        }
        return cfg

    @staticmethod
    def msg_log(robot_id, text):
        tk = TimeKit()
        now = tk.int2str(tk.now)
        try:
            text = text.replace("'", '"')
            sql = f'''insert into {T_MESSAGE} (robot_id, send_time, text) VALUES ('{robot_id}', '{now}', '{text}')'''
            ds.exe_sql(sql)
        except Exception as e:
            print(e)

    @staticmethod
    def msg(robot_id, text):
        time.sleep(1)
        robot_id = str(robot_id)
        if ',' in robot_id:
            robot_list = robot_id.split(',')
        else:
            robot_list = [robot_id]
        for rbt in robot_list:
            ding = Dingtalk(**ROBOTS[rbt])
            ding.send(content=text)
            Repo.msg_log(rbt, text)

    @staticmethod
    def admin_msg(text):
        time.sleep(1)
        ding = Dingtalk(**ROBOTS['2001'])
        ding.send(content=text)

    @staticmethod
    def get_job(id_list=None):
        if id_list is None:
            tk = TimeKit()
            # 添加逗号, 确保不会误匹配
            cur_month = ',' + str(int(str(tk.theday)[4:6])) + ','
            cur_week = ',' + str(tk.nday_of_week) + ','
            cur_day = ',' + str(int(str(tk.theday)[6:8])) + ','
            cur_hour = ',' + str(tk.hour) + ','
            cur_minute = ',' + str(tk.minute) + ','
            sql = f'''
            SELECT id, job_name, job_type, job_template, job_params, job_depend, message_robot, job_log, job_message, job_on_error
            FROM {T_JOB}
            WHERE (job_executor = {EXECUTOR} and job_status = 1 and (execution_status = 99 or (execution_status=-1 and error_count<retry_count)))
            or (
                job_executor = {EXECUTOR}
                and job_status = 1 
                and (execution_status = 0 or (execution_status=-1 and error_count<retry_count))
                and (job_schedule_month='*' or concat(',',job_schedule_month,',') like '%{cur_month}%')
                and (job_schedule_week='*' or concat(',',job_schedule_week,',') like '%{cur_week}%')
                and (job_schedule_day='*' or concat(',',job_schedule_day,',') like '%{cur_day}%')
                and (job_schedule_hour='*' or concat(',',job_schedule_hour,',') like '%{cur_hour}%')
                and (job_schedule_minute='*' or concat(',',job_schedule_minute,',') like '%{cur_minute}%')
            )
            '''
        else:
            id_list_str = ','.join([str(x) for x in id_list])
            sql = f'''
            SELECT id, job_name, job_type, job_template, job_params, job_depend, message_robot, job_log, job_message, job_on_error
            FROM {T_JOB}
            WHERE id in ({id_list_str})
            AND job_executor = {EXECUTOR}
            '''
        df = ds.read_sql(sql)
        return df.to_dict('records')

    @staticmethod
    def register_user_request(username, func_name):
        tk = TimeKit()
        now = tk.int2str(tk.now)
        sql = f"insert into {T_USER_REQUEST} (username,request_time,func_name) select '{username}' as username, '{now}' as request_time, '{func_name}' as func_name"
        ds.exe_sql(sql)

    @staticmethod
    def get_follow_job(id_list_str):
        sql = f'''
        SELECT id, job_name, job_type, job_template, job_params, job_depend, message_robot, job_log, job_message, job_on_error
        FROM {T_JOB}
        WHERE (execution_status in (0, 99) or (execution_status=-1 and error_count<retry_count))
        and job_status = 1
        and job_depend in ({id_list_str})
        '''
        df = ds.read_sql(sql)
        return df.to_dict('records')

    @staticmethod
    def register_job_pending(job_list):
        id_list_str = ','.join([str(x['id']) for x in job_list])
        sql1 = f'update {T_JOB} set execution_status=1 where id in ({id_list_str})'
        ds.exe_sql(sql1)

    @staticmethod
    def register_job_start(job_id):
        tk = TimeKit()
        now = tk.int2str(tk.now)
        sql1 = f"update {T_JOB} set execution_status=2, last_execution_time='{now}' where id = {job_id}"
        ds.exe_sql(sql1)

    @staticmethod
    def register_job_end(job_id):
        sql1 = f'update {T_JOB} set execution_status=0, error_count=0 where id = {job_id}'
        ds.exe_sql(sql1)

    @staticmethod
    def register_job_error(job_id):
        sql = f'update {T_JOB} set execution_status=-1, error_count=error_count+1 where id = {job_id}'
        ds.exe_sql(sql)

    @staticmethod
    def job_log_start(log_id, job_id, run_params_str):
        tk = TimeKit()
        now = tk.int2str(tk.now)
        try:
            sql = f'''
            insert into {T_JOB_LOG} (id, job_id, execution_status, start_time, job_params) 
            select '{log_id}' as log_id, id as job_id, 2 as execution_status, '{now}' as start_time, '{run_params_str}' as job_params
            from {T_JOB} 
            where id = {job_id}
            '''
            ds.exe_sql(sql)
        except Exception as e:
            print(e)

    @staticmethod
    def job_log_end(log_id, status, msg):
        tk = TimeKit()
        now = tk.int2str(tk.now)
        try:
            if msg is None:
                sql2 = f'''update {T_JOB_LOG} set execution_status={status}, end_time = '{now}' where id ='{log_id}' '''
            else:
                msg = str(msg).replace("'", "''").replace('\n', '    ')
                sql2 = f'''update {T_JOB_LOG} set execution_status={status}, message='{msg}', end_time = '{now}' where id ='{log_id}' '''
            ds.exe_sql(sql2)
        except Exception as e:
            print(e)

    @staticmethod
    def get_check_job(id_list_str):
        sql = f'''
        SELECT id, server_id, db_name, check_sql, robot_id
        FROM {T_CHECK}
        WHERE id in ({id_list_str})
        '''
        df = ds.read_sql(sql)
        return df.to_dict('records')

    @staticmethod
    def get_check_result(server_id, db_name, check_sql, run_params):
        ds_tgt = DataSource(**DATABASES[str(server_id)])
        ds_tgt.set_db(db_name)
        check_sql = check_sql.format(**run_params)
        v = ds_tgt.get_value(check_sql)
        v = v.replace("$rn", "\n")
        return str(v)

    @staticmethod
    def get_sync_job(id_list_str):
        sql = f'''
        SELECT id, from_server, from_db, from_sql, to_server, to_db, to_table, to_columns, before_write
        FROM {T_SYNC}
        WHERE id in ({id_list_str})
        '''
        df = ds.read_sql(sql)
        records = df.to_dict('records')
        id_list = [id.strip() for id in id_list_str.split(',')]
        ordered_records = sorted(records, key=lambda x: id_list.index(str(x['id'])))
        return ordered_records

    @staticmethod
    def get_py_job(id_list_str):
        sql = f'''
        SELECT id, py_path
        FROM {T_PY}
        WHERE id in ({id_list_str})
        '''
        df = ds.read_sql(sql)
        records = df.to_dict('records')
        id_list = [id.strip() for id in id_list_str.split(',')]
        ordered_records = sorted(records, key=lambda x: id_list.index(str(x['id'])))
        return ordered_records

    @staticmethod
    def get_flink_job(id_list_str):
        sql = f'''
        SELECT id, job_sql
        FROM {T_FLINK}
        WHERE id in ({id_list_str})
        '''
        df = ds.read_sql(sql)
        records = df.to_dict('records')
        id_list = [id.strip() for id in id_list_str.split(',')]
        ordered_records = sorted(records, key=lambda x: id_list.index(str(x['id'])))
        return ordered_records
    
    @staticmethod
    def get_sql_job(id_list_str):
        sql = f'''
        SELECT id, server_id, db_name, sql_text
        FROM {T_SQL}
        WHERE id in ({id_list_str})
        '''
        df = ds.read_sql(sql)
        records = df.to_dict('records')
        id_list = [id.strip() for id in id_list_str.split(',')]
        ordered_records = sorted(records, key=lambda x: id_list.index(str(x['id'])))
        return ordered_records

    @staticmethod
    def read_sync_data(server_id, db_name, sql_text, sql_params):
        ds_sync = DataSource(**DATABASES[str(server_id)])
        ds_sync.set_db(db_name)
        df = ds_sync.read_sql(sql_text.format(**sql_params))
        return df

    @staticmethod
    def exe_sync_sql(server_id, db_name, sql_text, sql_params):
        ds_sync = DataSource(**DATABASES[str(server_id)])
        ds_sync.set_db(db_name)
        ds_sync.exe_sql(sql_text.format(**sql_params))

    @staticmethod
    def write_sync_data(df, server_id, db_name, table_name):
        ds_sync = DataSource(**DATABASES[str(server_id)])
        ds_sync.set_db(db_name)
        if df is not None:
            ds_sync.to_db(df, table_name)

    @staticmethod
    def exe_sql_job(server_id, db_name, sql_text, sql_params):
        ds = DataSource(**DATABASES[str(server_id)])
        ds.set_db(db_name)
        # print(sql_text.format(**sql_params))
        ds.exe_sql(sql_text.format(**sql_params))

    @staticmethod
    def update_rpt_publish(rpt_id, rpt_period):
        tk = TimeKit()
        now = tk.int2str(tk.now)
        sql = f"update {T_RPT_PUB} set published=1,etl_time='{now}' where rpt_id={rpt_id} and rpt_period={rpt_period}"
        ds.exe_sql(sql)

    @staticmethod
    def submit_app_operation_job(job_id, params):
        sql = f'''update {T_JOB} set execution_status=99, job_params = '{params}' where id={job_id}'''
        ds.exe_sql(sql)
