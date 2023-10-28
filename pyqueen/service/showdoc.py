import requests


class Showdoc:
    def __init__(self, username, password, host):
        self.username = username
        self.password = password
        self.host = host

    def post(self, title, concat, cat_name=None):
        data = {
            'api_key': self.username,
            'api_token': self.password,
            'page_title': title,
            'page_content': concat
        }
        if cat_name is not None:
            data['cat_name'] = cat_name
        requests.post(self.host, data=data)
