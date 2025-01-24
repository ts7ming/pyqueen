

class DataSource:
    def __init__(self,
                 conn_type,
                 host=None,
                 username=None,
                 password=None,
                 port=None,
                 db_name=None,
                 db_type=None,
                 file_path=None,
                 jdbc_url=None,
                 cache_dir=None,
                 keep_conn=False,
                 charset=None,
                 conn_package=None,
                 conn_params=None
                 ):
        self.logger = None
        
