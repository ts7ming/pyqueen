import os
from pyqueen import DataSource
from cryptography.fernet import Fernet


class Config:
    def __init__(self,settings):
        self.ds_cfg = DataSource(**settings.DS_CONFIG)

    @staticmethod
    def encrypt(text, key):
        cipher = Fernet(key.encode('utf-8'))
        return cipher.encrypt(text.encode('utf-8')).decode('utf-8')

    @staticmethod
    def decrypt(text, key):
        cipher = Fernet(key.encode('utf-8'))
        return cipher.decrypt(text).decode('utf-8')

    def get_dict(self, key=None):
        




# --------------------------  配置库  -----------------------------
ds_cfg = DataSource(**settings.DS_CONFIG) if hasattr(settings, 'DS_CONFIG') else None

# --------------------------  配置表表名  --------------------------
T_SERVER = getattr(settings, 'T_SERVER', 'etl_server')
T_JOB = getattr(settings, 'T_JOB', 'etl_job')
T_JOB_LOG = getattr(settings, 'T_JOB_LOG', 'etl_log')
T_CHECK = getattr(settings, 'T_CHECK', 'etl_job_check')
T_SYNC = getattr(settings, 'T_SYNC', 'etl_job_sync')
T_PY = getattr(settings, 'T_PY', 'etl_job_py')
T_FLINK = getattr(settings, 'T_FLINK', 'etl_job_flink')
T_SQL = getattr(settings, 'T_SQL', 'etl_job_sql')
T_MESSAGE = getattr(settings, 'T_MESSAGE', 'etl_robot_message')
T_DICT = getattr(settings, 'T_DICT', 'etl_dict')
T_RPT_PUB = getattr(settings, 'T_RPT_PUB', 'etl_rpt_publish')
T_USER_REQUEST = getattr(settings, 'T_USER_REQUEST', 'etl_user_request')

# --------------------------  环境参数  ----------------------------
SECRET_KEY = getattr(settings, 'SECRET_KEY', 'SECRET_KEY')
WORK_DIR = getattr(settings, 'WORK_DIR', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENVIRONMENT_VARIABLE = getattr(settings, 'ENVIRONMENT_VARIABLE', [])
FINEREPORT_DIR = getattr(settings, 'FINEREPORT_DIR', None)
DATAX_PY = getattr(settings, 'DATAX_PY', None)
PY_PATH = getattr(settings, 'PY_PATH', 'python3')
FLINK_CONFIG = getattr(settings, 'FLINK_CONFIG', None)
EXECUTOR = getattr(settings, 'EXECUTOR', '0')
DEBUG = getattr(settings,'DEBUG',False)

# --------------------------  数据连接  ----------------------------
if hasattr(settings, 'DATABASES'):
    DATABASES = settings.DATABASES
else:
    DATABASES = {}
if len(DATABASES)==0 and ds_cfg is not None:
    tmp_db = {}
    sql = f'select server_id,conn_type,host,username,password,port,db_name from {T_SERVER}'
    df = ds_cfg.read_sql(sql)
    for server_id, conn_type, host, username, password, port, db_name in df.values:
        tmp_db[str(server_id)] = {
            'conn_type': conn_type,
            'host': host,
            'username': username,
            'password': decrypt(password, SECRET_KEY),
            'port': port,
            'db_name': db_name
        }
    for k,v in tmp_db.items():
        if k not in DATABASES.keys():
            DATABASES[k]=v

# --------------------------  通知机器人  ----------------------------
if hasattr(settings, 'ROBOTS'):
    ROBOTS = settings.ROBOTS
else:
    ROBOTS = {}
    if ds_cfg is not None:
        sql = '''
        select robot_id, access_token, secret
        from etl_robot
        '''
        df = ds_cfg.read_sql(sql)
        for robot_id, access_token, secret in df.values:
            ROBOTS[str(robot_id)] = {
                'access_token': access_token,
                'secret': secret
            }

if __name__ == '__main__':
    pw = input('password:')
    pw = str(pw).strip()
    epw = encrypt(pw, SECRET_KEY)
    if decrypt(epw, SECRET_KEY) == pw:
        print(f'encrypt password: "{epw}"')
    else:
        print('校验失败')
