import os.path


class Web:
    def __init__(self, cache_dir=None):
        self.__cache_dir = str(cache_dir).replace('\\', '/')
        if self.__cache_dir[-1] != '/':
            self.__cache_dir += '/'

    def read_page(self, url):
        if self.__cache_dir is None:
            return self.__read_page_from_source(url)
        else:
            if self.__check_temp(url):
                return self.__read_page_from_local(url)
            else:
                page = self.__read_page_from_source(url)
                self.__save_cache(url, page)
                return page

    @staticmethod
    def __read_page_from_source(url):
        import requests
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit537.36 (KHTML, like Gecko) Chrome / 54.0.2840.99 Safari / 537.36"}
        response = requests.get(url, headers=headers)
        html = response.text
        print('from source')
        return html

    def __read_page_from_local(self, url):
        with open(self.__url2path(url), 'r', encoding='utf-8') as f:
            page = f.read()
        print('from local')
        return page

    def __save_cache(self, url, page):
        path = self.__url2path(url)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(str(page))

    def __url2path(self, url):
        if len(url) > 200:
            f_url = url.replace('://', '').replace('/', '_').replace('?', '_').replace('.', '')[0:200]
            f_url = self.__cache_dir + f_url + '.pqtmp'
            return f_url
        else:
            f_url = url.replace('://', '').replace('/', '_').replace('?', '_').replace('.', '')
            f_url = self.__cache_dir + f_url + '.pqtmp'
            return f_url

    def __check_temp(self, url):
        tmp_path = self.__url2path(url)
        if os.path.exists(tmp_path):
            return True
        else:
            return False
