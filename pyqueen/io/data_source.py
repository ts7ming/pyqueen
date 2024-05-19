import warnings
import pandas as pd

warnings.simplefilter(action='always', category=PendingDeprecationWarning)

__support_conn_type__ = ('mysql', 'mssql', 'oracle', 'clickhouse', 'sqlite', 'pgsql', 'postgresql')
__conn_type_mapping__ = {
    'mysql': 'MySQL',
    'oracle': 'Oracle',
    'mssql': 'MSSQL',
    'clickhouse': 'Clickhouse',
    'pgsql': 'PostgresSQL',
    'postgresql': 'PostgresSQL',
    'sqlite': 'Sqlite',
    'jdbc': 'SqlDB'

}


class DataSource:
    def __init__(self, conn_type=None, host=None, username=None, password=None, port=None, db_name=None, db_type=None, file_path=None, jdbc_url=None):
        if conn_type is None and db_type is None:
            raise Exception('missing conn_type! supported conn_type:' + ','.join(__support_conn_type__))
        if conn_type is None and db_type is not None:
            warnings.warn(message="recommend using the 'conn_type' field instead of 'db_type'", category=PendingDeprecationWarning)
            conn_type = db_type
        conn_type = str(conn_type).lower()
        operator = __conn_type_mapping__[conn_type]
        if conn_type in ('mysql', 'mssql', 'oracle', 'clickhouse', 'sqlite', 'postgresql', 'pgsql', 'jdbc'):
            from pyqueen.io import sqldb
            self.operator = getattr(sqldb, operator)(host=host, username=username, password=password, port=port, db_name=db_name)
        elif conn_type in ('excel',):
            from pyqueen.io import excel
            self.operator = getattr(excel, operator)(file_path=file_path)
        elif conn_type in ('redis',):
            from pyqueen.io import kvdb
            self.operator = getattr(kvdb, operator)(file_path=file_path)
        else:
            raise Exception('Unknow conn_type')

    def get_sql(self, sql):
        return self.operator.read_sql(sql)

    def read_sql(self, sql):
        return self.operator.read_sql(sql)

    def exe_sql(self, sql):
        self.operator.exe_sql(sql)

    def to_db(self, df, tb_name, how='append', fast_load=False, chunksize=10000):
        self.operator.to_db(df=df, tb_name=tb_name, how=how, fast_load=fast_load, chunksize=chunksize)

    def get_sheets(self):
        return self.operator.get_sheets()

    def get_v(self, key):
        return self.operator.get_v(key)

    def set_v(self, key, value):
        return self.operator.set_v(key, value)

    def row_count(self, table_name):
        sql = 'select count(1) from ' + table_name
        df = self.read_sql(sql)
        rows = df.values[0][0]
        return int(rows)

    def get_sql_group(self, sql, params):
        df = None
        for param in params:
            tsql = sql.format(param)
            df_tmp = self.get_sql(tsql)
            if df is None:
                df = df_tmp
            else:
                df = pd.concat([df, df_tmp])
        return df
