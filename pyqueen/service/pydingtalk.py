from urllib3 import encode_multipart_formdata
from copy import copy
import requests
import json
import os
import time
import hmac
import hashlib
import base64
import urllib.parse


class Dingtalk:
    def __init__(self, access_token, secret=None, corpid=None, corpsecret=None):
        self.header = {"Content-Type": "application/json"}
        self.access_token = access_token
        self.secret = secret
        self.corpid = corpid
        self.corpsecret = corpsecret
        self.msg = {}
        self.url = self.__get_url()

    def __get_url(self):
        if self.secret is None:
            url = "https://oapi.dingtalk.com/robot/send?access_token=%s" % str(self.access_token)
            return url
        timestamp = str(round(time.time() * 1000))
        secret_enc = self.secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        url = "https://oapi.dingtalk.com/robot/send?access_token={a}&timestamp={t}&sign={s}".format(a=self.access_token, t=timestamp, s=sign)
        return url

    def __getToken(self):
        url = 'https://oapi.dingtalk.com/gettoken?corpid=%s&corpsecret=%s' % (self.corpid, self.corpsecret)
        req = requests.get(url)
        access_token = json.loads(req.text)
        access_token = access_token['access_token']
        return access_token

    def send_text(self, content, mentioned_list=None, mentioned_mobile_list=None):
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
                raise Exception('钉钉发送失败')
        except Exception as ee:
            raise Exception('钉钉发送失败：%s' % ee)

    def send(self, content=None, mentioned_list=None, mentioned_mobile_lis=None, file_path=None, img_path=None):
        if content is not None:
            if mentioned_list is None:
                mentioned_list = []
            if mentioned_mobile_lis is None:
                mentioned_mobile_lis = []
            self.send_text(content, mentioned_list, mentioned_mobile_lis)
        if file_path is not None:
            self.send_file(file_path)
        if img_path is not None:
            with open(img_path, "rb") as f:
                md = hashlib.md5()
                md.update(f.read())
                image_md5 = md.hexdigest()
            with open(img_path, "rb") as f:
                image_data = str(base64.b64encode(f.read()), 'utf-8')
            self.send_image(image_data, image_md5)

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
                raise Exception('钉钉发送失败')
        except Exception as ee:
            raise Exception('钉钉发送失败：%s' % ee)

    def send_file(self, file_path):
        """
        文件类型
        :param file_path: 文件路径
        :return:
        """
        file_path = str(file_path).replace('\\', '/')
        upload_url = 'https://oapi.dingtalk.com/media/upload?access_token=%s' % self.__getToken()
        print(upload_url)
        file_name = file_path.split("/")[-1]
        with open(file_path, 'rb') as f:
            length = os.path.getsize(file_path)
            data = f.read()
        headers = {'Content-Type': 'application/octet-stream'}
        params = {'filename': file_name, 'filelength': length}
        file_data = copy(params)
        file_data['media'] = (file_path.split('/')[-1:][0], data)
        # file_data = {'type':'file','media':file_path}
        encode_data = encode_multipart_formdata(file_data)
        file_data = encode_data[0]
        headers['Content-Type'] = encode_data[1]
        upload_res = json.loads(requests.post(url=upload_url, data=file_data, headers=headers).text)
        # upload_res = json.loads(requests.post(url=upload_url, data=file_data).text)
        print(upload_res)
        self.msg['type'] = 'file'
        self.msg['file'] = {}
        self.msg['file']['media_id'] = upload_res['media_id']
        res = json.loads(requests.post(url=self.url, data=json.dumps(self.msg), headers=self.header).text)
        if res['errmsg'] != 'ok':
            raise Exception('钉钉发送失败')
