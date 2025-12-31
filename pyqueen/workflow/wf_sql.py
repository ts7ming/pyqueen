class WfSql:
    def __init__(self, ds, sql, params=None):
        self.ds = ds
        self.sql_text = sql
        self.sql_params = params

    def sql(self):
        if self.sql_params is None:
            real_sql = self.sql_text
        else:
            real_sql = self.sql_text.format(**self.sql_params)
        self.ds.exe_sql(real_sql)
        return real_sql
