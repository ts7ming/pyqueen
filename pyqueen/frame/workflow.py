from ..io.data_source import DataSource


class Workflow:
    def __init__(self, settings):
        self.__SERVERS = settings['SERVERS']

    def read(self, server_id, sql):
        pass

    