from ctypes import *
import os


class FTP:
    def __init__(self, username, password, host, port, encoding='utf-8'):
        self.__ftp = None
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.encoding = encoding

    def __download_file(self, local_file, remote_file):  # 下载单个文件
        file_handler = open(local_file, 'wb')
        self.__ftp.retrbinary('RETR ' + remote_file, file_handler.write)
        file_handler.close()
        return True

    def __download_folder(self, local_dir, remote_dir):  # 下载整个目录下的文件
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        self.__ftp.cwd(remote_dir)
        remote_names = self.__ftp.nlst()
        for file in remote_names:
            local = os.path.join(local_dir, file)
            if file.find(".") == -1:
                if not os.path.exists(local):
                    os.makedirs(local)
                self.__download_folder(local, file)
            else:
                self.__download_file(local, file)
        self.__ftp.cwd("..")

    def download_dir(self, local_dir, remote_dir):
        import ftplib
        self.__ftp = ftplib.FTP()
        self.__ftp.connect(self.host, self.port)
        self.__ftp.encoding = self.encoding
        self.__ftp.login(self.username, self.password)
        self.__download_folder(local_dir, remote_dir)  # 递归执行
        self.__ftp.quit()
