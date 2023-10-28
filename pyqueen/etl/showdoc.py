import requests
import json

class Showdoc:
    def __init__(self, username, password, host):
        self.username = username
        self.password = password
        self.host = host
        self.__operator = None

    def set_operator(self, func):
        self.__operator = func

    def post(self, title, concat, cat_name = None):
        data = {
            'api_key': self.username,
            'api_token': self.password,
            'page_title': title,
            'page_content': concat
        }
        if cat_name is not None:
            data['cat_name'] = cat_name
        requests.post(self.host, data=data)
        if self.__operator is not None:
            try:
                self.__operator({'title': title, 'func': 'pykoala.Showdoc.post'})
            except:
                pass