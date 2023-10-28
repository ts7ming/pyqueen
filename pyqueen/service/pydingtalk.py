from urllib3 import encode_multipart_formdata
from copy import copy
import requests
import json
import os


class Dingtalk:
    """
    发送微信
    """

    def __init__(self, access_token):
        """
        初始化
        :param key: 微信机器人key
        """
        self.url = "https://oapi.dingtalk.com/robot/send?access_token=%s" % str(access_token)
        self.header = {"Content-Type": "application/json"}
        self.msg = {}
        self.__operator = None

    def set_operator(self, func):
        self.__operator = func

    def send_text(self,
                  content,
                  mentioned_list=None,
                  mentioned_mobile_list=None):
        """
        文本类型
        :param content: 文本内容，最长不超过2048个字节，必须是utf8编码
        :param mentioned_list: userid的列表，提醒群中的指定成员(@某个成员)，@all表示提醒所有人，如果开发者获取不到userid，可以使用mentioned_mobile_list
        :param mentioned_mobile_list: 手机号列表，提醒手机号对应的群成员(@某个成员)，@all表示提醒所有人
        :return:
        """
        try:
            self.msg['msgtype'] = 'text'
            self.msg['text'] = {}
            self.msg['text']['content'] = content
            if mentioned_list:
                self.msg['text']['mentioned_list'] = mentioned_list
            if mentioned_mobile_list:
                self.msg['text'][
                    'mentioned_mobile_list'] = mentioned_mobile_list
            res = json.loads(
                requests.post(url=self.url,
                              data=json.dumps(self.msg),
                              headers=self.header).text)
            if res['errmsg'] != 'ok':
                raise Exception('微信发送失败')
        except Exception as ee:
            raise Exception('微信发送失败：%s' % ee)
        if self.__operator is not None:
            try:
                self.__operator({'content': content, 'mentioned_list': mentioned_list, 'func': 'pymro.Wechat.send_text'})
            except:
                pass

    def send_markdown(self, content):
        """
        markdown类型
        :param content: markdown内容，最长不超过4096个字节，必须是utf8编码
        :return:
        """
        try:
            self.msg['msgtype'] = 'markdown'
            self.msg['markdown'] = {}
            self.msg['markdown']['content'] = content
            res = json.loads(
                requests.post(url=self.url,
                              data=json.dumps(self.msg),
                              headers=self.header).text)
            if res['errmsg'] != 'ok':
                raise Exception('微信发送失败')
        except Exception as ee:
            raise Exception('微信发送失败：%s' % ee)
        if self.__operator is not None:
            try:
                self.__operator({'content': content, 'func': 'pymro.Wechat.send_markdown'})
            except:
                pass

    def send_image(self, base64, md5):
        """
        图片类型
        :param base64: 图片内容的base64编码
        :param md5: 图片内容（base64编码前）的md5值
        :return:
        """
        try:
            self.msg['msgtype'] = 'image'
            self.msg['image'] = {}
            self.msg['image']['base64'] = base64
            self.msg['image']['md5'] = md5
            res = json.loads(
                requests.post(url=self.url,
                              data=json.dumps(self.msg),
                              headers=self.header).text)
            if res['errmsg'] != 'ok':
                raise Exception('微信发送失败')
        except Exception as ee:
            raise Exception('微信发送失败：%s' % ee)
        if self.__operator is not None:
            try:
                self.__operator({'func': 'pymro.Wechat.send_image'})
            except:
                pass

    def send_file(self, file_path):
        """
        文件类型
        :param file_path: 文件路径
        :return:
        """
        try:
            upload_url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key=%s&type=file' % self.key
            file_name = file_path.split("/")[-1]
            with open(file_path, 'rb') as f:
                length = os.path.getsize(file_path)
                data = f.read()
            headers = {'Content-Type': 'application/octet-stream'}
            params = {'filename': file_name, 'filelength': length}
            file_data = copy(params)
            file_data['file'] = (file_path.split('/')[-1:][0], data)
            encode_data = encode_multipart_formdata(file_data)
            file_data = encode_data[0]
            headers['Content-Type'] = encode_data[1]
            upload_res = json.loads(
                requests.post(url=upload_url, data=file_data,
                              headers=headers).text)
            self.msg['msgtype'] = 'file'
            self.msg['file'] = {}
            self.msg['file']['media_id'] = upload_res['media_id']
            res = json.loads(
                requests.post(url=self.url,
                              data=json.dumps(self.msg),
                              headers=self.header).text)
            if res['errmsg'] != 'ok':
                raise Exception('微信发送失败')
        except Exception as ee:
            raise Exception('微信发送失败：%s' % ee)
        if self.__operator is not None:
            try:
                self.__operator({'file_path': file_path, 'func': 'pymro.Wechat.send_file'})
            except:
                pass