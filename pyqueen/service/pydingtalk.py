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

    def send_text(self, content, mentioned_list=None, mentioned_mobile_list=None, mentioned_all=False):
        try:
            self.msg['msgtype'] = 'text'
            self.msg['text'] = {}
            self.msg['text']['content'] = content
            if mentioned_all:
                self.msg['at']['isAtAll'] = 'true'
            elif mentioned_list:
                self.msg['at']['atUserIds'] = mentioned_list
            elif mentioned_mobile_list:
                self.msg['at']['atMobiles'] = mentioned_mobile_list
            res = json.loads(
                requests.post(url=self.url,
                              data=json.dumps(self.msg),
                              headers=self.header).text)
            if res['errmsg'] != 'ok':
                raise Exception('钉钉发送失败')
        except Exception as ee:
            raise Exception('钉钉发送失败：%s' % ee)

    def send_markdown(self, title, text, mentioned_list=None, mentioned_mobile_list=None, mentioned_all=False):
        try:
            self.msg['msgtype'] = 'markdown'
            self.msg['markdown'] = {}
            self.msg['markdown']['title'] = title
            self.msg['markdown']['text'] = text
            if mentioned_all:
                self.msg['at']['isAtAll'] = 'true'
            elif mentioned_list:
                self.msg['at']['atUserIds'] = mentioned_list
            elif mentioned_mobile_list:
                self.msg['at']['atMobiles'] = mentioned_mobile_list
            res = json.loads(
                requests.post(url=self.url,
                              data=json.dumps(self.msg),
                              headers=self.header).text)
            if res['errmsg'] != 'ok':
                raise Exception('钉钉发送失败')
        except Exception as ee:
            raise Exception('钉钉发送失败：%s' % ee)
        
    def send(self, content=None, mentioned_list=None, mentioned_mobile_lis=None, mentioned_all=False, file_path=None, img_path=None):
        self.send_text(content=content, mentioned_list=mentioned_list, mentioned_mobile_list=mentioned_mobile_lis, mentioned_all=mentioned_all)
