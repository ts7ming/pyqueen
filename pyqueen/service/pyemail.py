from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


class Email:
    """
    发送邮件
    """

    def __init__(self, username, password, host, port):
        """
        初始化
        :param username: 邮箱账号
        :param password: 邮箱密码
        :param host: 地址
        :param port: 端口
        """
        self.username = username
        self.password = password
        self.host = host
        self.port = port

    def __conn(self):
        try:
            self.smtp = smtplib.SMTP_SSL(host=self.host, port=self.port)
            self.smtp.login(user=self.username, password=self.password)
        except Exception as ee:
            raise Exception('邮箱登录失败：%s' % ee)

    def send_text(self,
                  subject,
                  content,
                  to_user: list,
                  cc_user: list = None,
                  bcc_user: list = None,
                  type='plain'):
        """
        发送文本邮件
        :param subject: 邮件主题
        :param content: 邮件内容
        :param to_user: 收件人
        :param cc_user: 抄送人
        :param bcc_user: 密抄人
        :param type: 文本或html格式，默认文本格式
        :return:
        """
        self.__conn()
        try:
            msg = MIMEText(content, _subtype=type, _charset='utf-8')
            msg['subject'] = subject
            msg['From'] = 'name <name>'.format(name=self.username)
            msg['To'] = ','.join(to_user)
            if cc_user:
                msg['Cc'] = ','.join(cc_user)
            if bcc_user:
                msg['Bcc'] = ','.join(bcc_user)
            self.smtp.send_message(msg, from_addr=self.username)
        except Exception as ee:
            raise Exception('邮件发送失败：%s' % ee)

    def send_file(self,
                  subject,
                  content,
                  file_path_list: list,
                  to_user: list,
                  cc_user: list = None,
                  bcc_user: list = None,
                  type='plain'):
        """
        发送附件邮件
        :param subject: 邮件主题
        :param content: 邮件内容
        :param file_path_list: 附件路径列表
        :param to_user: 收件人
        :param cc_user: 抄送人
        :param bcc_user: 密抄人
        :param type: 文本或html格式，默认文本格式
        :return:
        """
        self.__conn()
        try:
            msg = MIMEMultipart()
            msg['subject'] = subject
            msg['From'] = 'name <name>'.format(name=self.username)
            msg['To'] = ','.join(to_user)
            if cc_user:
                msg['Cc'] = ','.join(cc_user)
            if bcc_user:
                msg['Bcc'] = ','.join(bcc_user)
            text_msg = MIMEText(content, _subtype=type, _charset='utf-8')
            msg.attach(text_msg)
            for file_path in file_path_list:
                file_path = str(file_path).replace('\\', '/')
                file_content = open(file_path, 'rb').read()
                file_msg = MIMEApplication(file_content)
                file_msg.add_header('content-disposition',
                                    'attachment',
                                    filename=('gbk', '',
                                              '%s' % file_path.split('/')[-1]))
                msg.attach(file_msg)
            self.smtp.send_message(msg, from_addr=self.username)
        except Exception as ee:
            raise Exception('邮件发送失败：%s' % ee)
