from ctypes import *
import os
import ftplib


class FtpDownloader:
    def __init__(self, username, password, host, port):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.__operator = None

    def set_operator(self, func):
        self.__operator = func

    def __download_file(self, local_file, remote_file):  # 下载单个文件
        file_handler = open(local_file, 'wb')
        self.ftp.retrbinary('RETR ' + remote_file, file_handler.write)
        file_handler.close()
        return True

    def __download_folder(self, local_dir, remote_dir):  # 下载整个目录下的文件
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        self.ftp.cwd(remote_dir)
        remote_names = self.ftp.nlst()
        for file in remote_names:
            local = os.path.join(local_dir, file)
            if file.find(".") == -1:
                if not os.path.exists(local):
                    os.makedirs(local)
                self.__download_folder(local, file)
            else:
                self.__download_file(local, file)
        self.ftp.cwd("..")

    def download_folder(self, local_dir, remote_dir):
        self.ftp = ftplib.FTP()
        self.ftp.connect(self.host, self.port)
        self.ftp.encoding = 'utf-8'
        self.ftp.login(self.username, self.password)

        self.__download_folder(local_dir, remote_dir)  # 递归执行

        self.ftp.quit()
        if self.__operator is not None:
            try:
                self.__operator({'local_dir': local_dir, 'remote_dir': remote_dir, 'func': 'pykoala.FtpDownloader.download_folder'})
            except:
                pass
