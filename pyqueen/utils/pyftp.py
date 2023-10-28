from ftplib import error_perm
from ftplib import FTP
import socket
import os



class Ftp:
    """
    FTP基础类
    """

    def __init__(self, host, port, username, password):
        """
        ftp初始化
        :param host: ip地址
        :param port: 端口
        :param username: 用户名
        :param password: 密码
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ftp = FTP()
        # 传输字节、编码格式
        self.buf_size = 1024
        self.code = 'utf-8'

    def create_connect(self):
        """
        创建ftp连接
        :return:
        """
        try:
            self.ftp.connect(self.host, self.port)
            self.ftp.set_pasv(True)
            self.ftp.login(self.username, self.password)
            self.ftp.encoding = self.code
        except Exception as ee:
            raise Exception('ftp登陆失败：%s' % ee)

    def upload_file(self, remote_path, local_path):
        """
        ftp上传
        :param remote_path: 远程服务器路径
        :param local_path: 本地路径
        :return:
        """
        try:
            fp = open(local_path, 'rb')
            res = self.ftp.storbinary('STOR ' + remote_path, fp, self.buf_size)
            if res.find('226') != -1:
                self.ftp.set_debuglevel(0)
            fp.close()
        except Exception as ee:
            raise Exception('ftp上传失败：%s' % ee)

    def download_file(self, remote_path, local_path):
        """
        ftp下载
        :param remote_path: 远程服务器路径
        :param local_path: 本地路径
        :return:
        """
        try:
            fp = open(local_path, 'wb')
            res = self.ftp.retrbinary('RETR ' + remote_path, fp.write, self.buf_size)
            if res.find('226') != -1:
                self.ftp.set_debuglevel(0)
                fp.close()
        except Exception as ee:
            raise Exception('ftp下载失败：%s' % ee)

    def cd_remote_path(self, remote_path):
        """
        ftp进入指定目录
        :param remote_path: 远程服务器路径
        :return:
        """
        try:
            self.ftp.cwd(remote_path)
        except Exception as ee:
            raise Exception('ftp找不到相关路径：%s' % ee)

    def loop_remote_list(self, remote_path):
        """
        ftp获取当前目录下所有文件
        :param remote_path: 远程服务器路径
        :return: 当前目录下所有文件  type:list
        """
        try:
            self.cd_remote_path(remote_path)
            return [os.path.join(remote_path, path) for path in self.ftp.nlst()]
        except Exception as ee:
            raise Exception('ftp找不到相关路径：%s' % ee)

    def move_remote_file(self, from_path, to_path):
        """
        ftp移动文件
        :param from_path: 原始路径
        :param to_path: 最终路径
        :return:
        """
        try:
            self.ftp.rename(from_path, to_path)
        except Exception as ee:
            self.ftp.delete(to_path)
            self.ftp.rename(from_path, to_path)