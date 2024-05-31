class KvDB:
    def __init__(self, conn_type, host, port, db_name, charset='utf-8', keep_conn=False):
        self.__conn_type = conn_type
        self.__conn = None
        self.__host = host
        self.__port = port
        self.__db_name = db_name
        self.__charset = charset
        self.__keep_conn = keep_conn

    def create_conn(self):
        if self.__keep_conn is False or self.__conn is None:
            if str(self.__conn_type).lower() == 'redis':
                import redis
                self.__conn = redis.Redis(host=self.__host, port=self.__port, db=self.__db_name)
            else:
                raise Exception('Unknown conn_type: ' + str(self.__conn_type))

    def close_conn(self):
        if self.__keep_conn is False:
            if str(self.__conn_type).lower() == 'redis':
                self.__conn.close()
            else:
                raise Exception('Unknown conn_type: ' + str(self.__conn_type))

    def get_v(self, key):
        self.create_conn()
        value = self.__conn.get(key)
        self.close_conn()
        if value is None:
            return None
        value = value.decode(encoding=self.__charset)
        return value

    def set_v(self, key, value):
        self.create_conn()
        self.__conn.set(key, value)
        self.close_conn()
