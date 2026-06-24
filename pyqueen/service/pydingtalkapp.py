import requests
import json


class DingtalkApp:
    def __init__(self, agent_id, app_key, app_secret):
        self.agent_id = agent_id
        self.app_key = app_key
        self.app_secret = app_secret
        self.user_id = None
        self.union_id = None
        self.access_token = None

    def set_user_id(self, user_id=None, moble=None):
        if user_id is None:
            self.user_id = self.__get_user_id(mobile=moble)
        else:
            self.user_id = user_id
        self.union_id = None  # 重置 union_id

    def mobile2uid(self, mobile):
        return self.__get_user_id(mobile)

    def __get_user_id(self, mobile):
        if not self.access_token:
            self.access_token = self.__get_access_token()

        url = "https://oapi.dingtalk.com/topapi/v2/user/getbymobile"
        params = {"access_token": self.access_token}
        data = {"mobile": mobile}

        response = requests.post(url, params=params, json=data).json()

        if response.get("errcode") == 0:
            return response.get("result").get("userid")
        else:
            raise Exception(f"获取 user_id 失败: {response}")

    def __get_dentry_id(self, space_id, folder_name, parent_dentry_id):
        if not self.access_token:
            self.access_token = self.__get_access_token()
        url = f"https://api.dingtalk.com/v1.0/storage/spaces/{space_id}/dentries?parentId={parent_dentry_id}&unionId={self.union_id}"
        headers = {
            "x-acs-dingtalk-access-token": self.access_token,
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            info = response.json()["dentries"]
            for dentry in info:
                if dentry["name"] == folder_name:
                    dentry_id = dentry["id"]
                    return dentry_id
        else:
            raise Exception(f"获取 dentry_uuid 失败: {response.text}")

    def __dentry_id2uuid(self, space_id, dentry_id):
        if not self.access_token:
            self.access_token = self.__get_access_token()
        url = f"https://api.dingtalk.com/v2.0/doc/dentries/{dentry_id}/queryDentryUuid?spaceId={space_id}&operatorId={self.union_id}"
        headers = {
            "x-acs-dingtalk-access-token": self.access_token,
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            dentry_uuid = response.json()["dentryUuid"]
            return dentry_uuid
        else:
            raise Exception(f"获取 dentry_uuid 失败: {response.text}")

    def __get_dentry_uuid(self, space_id, folder_path):
        if not self.union_id:
            self.union_id = self.__get_union_id()
        if '/' not in folder_path:
            dentry_id = self.__get_dentry_id(
                space_id=space_id, folder_name=folder_path, parent_dentry_id='0')
        else:
            tmp_id = None
            parent_dentry_id = '0'
            for item in folder_path.split('/'):
                tmp_id = self.__get_dentry_id(
                    space_id=space_id, folder_name=item, parent_dentry_id=parent_dentry_id)
                parent_dentry_id = tmp_id
            dentry_id = tmp_id
        dentry_uuid = self.__dentry_id2uuid(space_id, dentry_id)
        return dentry_uuid

    def __get_upload_info(self, dentry_uuid, file_name):
        if not self.union_id:
            self.union_id = self.__get_union_id()
        if not self.access_token:
            self.access_token = self.__get_access_token()
        url = f"https://api.dingtalk.com/v2.0/storage/spaces/files/{dentry_uuid}/uploadInfos/query?unionId={self.union_id}"
        headers = {
            "x-acs-dingtalk-access-token": self.access_token,
            "Content-Type": "application/json"
        }
        data = {
            "protocol": "HEADER_SIGNATURE",
            "option": {
                "storageDriver": "DINGTALK",
                "preCheckParam": {
                    "size": 512,
                    "name": file_name
                }
            }
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        return response.json()

    def __upload_to_oss(self, resource_url, oss_headers, file_path):
        with open(file_path, "rb") as f:
            file_data = f.read()

        response = requests.put(
            resource_url, headers=oss_headers, data=file_data)
        # OSS 返回 200 表示上传成功
        return response.status_code == 200

    def __commit_file(self, dentry_uuid, upload_key, file_name):
        if not self.union_id:
            self.union_id = self.__get_union_id()
        if not self.access_token:
            self.access_token = self.__get_access_token()
        url = f"https://api.dingtalk.com/v2.0/storage/spaces/files/{dentry_uuid}/commit?unionId={self.union_id}"
        headers = {
            "x-acs-dingtalk-access-token": self.access_token,
            "Content-Type": "application/json"
        }
        data = {
            "uploadKey": upload_key,
            "name": file_name,
            "option": {
                "conflictStrategy": "AUTO_RENAME"  # 文件名冲突时自动重命名
            }
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        return response.json()

    def __get_access_token(self):
        """
        获取企业内部应用的 access_token
        """
        url = "https://oapi.dingtalk.com/gettoken"
        params = {
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        response = requests.get(url, params=params).json()

        if response.get("errcode") == 0:
            return response.get("access_token")
        else:
            raise Exception(f"获取 access_token 失败: {response}")

    def __get_space_id(self, space_name):
        if not self.union_id:
            self.union_id = self.__get_union_id()
        if not self.access_token:
            self.access_token = self.__get_access_token()
        url = "https://api.dingtalk.com/v1.0/drive/spaces"
        headers = {
            "x-acs-dingtalk-access-token": self.access_token,
            "Content-Type": "application/json"
        }
        params = {
            "unionId": self.union_id,
            "spaceType": "org",  # 查询企业空间
            "maxResults": 50     # 分页大小，最大50
        }
        response = requests.get(url, headers=headers, params=params).json()
        if response:
            spaces = response.get("spaces", [])
            if spaces:
                for space in spaces:
                    if space.get("spaceName") == space_name:
                        return space.get("spaceId")
            else:
                raise Exception("未找到")
        else:
            raise Exception(f"获取 space_id 失败: {response}")

    def __get_union_id(self):
        """
        根据 user_id 获取用户的 union_id
        """
        if not self.access_token:
            self.access_token = self.__get_access_token()
        url = "https://oapi.dingtalk.com/topapi/v2/user/get"
        params = {"access_token": self.access_token}
        data = {"userid": self.user_id}

        response = requests.post(url, params=params, json=data).json()

        if response.get("errcode") == 0:
            return response.get("result").get("unionid")
        else:
            raise Exception(f"获取 union_id 失败: {response}")

    def upload_file(self, remote_folder, local_file_path):
        """上传文件到钉钉云盘

        将本地文件上传到指定的钉钉云盘目录。会自动处理认证、获取目录信息、
        上传文件到OSS、提交文件等所有步骤。

        Args:
            remote_folder: 远程文件夹路径，格式为 "空间名/文件夹路径"
                          例如: "公共空间/项目文档/报告"
                          如果文件夹不存在，需要先创建
            local_file_path: 本地文件路径
                           例如: "C:/files/report.xlsx" 或 "./data/file.pdf"
                           支持 Windows 和 Unix 路径格式

        Example:
            >>> app = DingtalkApp(agent_id="123456", app_key="ding123", app_secret="secret123")
            >>> app.set_user_id("user123")
            >>> app.upload_file(
            ...     remote_folder="公共空间/项目文档",
            ...     local_file_path="./files/report.xlsx"
            ... )
        """
        if self.user_id is None:
            raise Exception('请设置用户: DingtalkApp.set_user_id')
        # 解析远程文件夹路径（空间名/文件夹路径）
        if '/' in remote_folder:
            parts = remote_folder.split('/', 1)
            space_name = parts[0]
            folder_path = parts[1]
        else:
            space_name = remote_folder
            folder_path = ''

        # 获取空间ID
        space_id = self.__get_space_id(space_name=space_name)

        # 标准化文件夹路径
        folder_path = folder_path.replace(
            '\\\\', '/').replace('\\', '/').rstrip('/')
        dentry_uuid = self.__get_dentry_uuid(space_id, folder_path)
        # 确保已获取 union_id 和 access_token
        if not self.union_id:
            self.union_id = self.__get_union_id()
        if not self.access_token:
            self.access_token = self.__get_access_token()

        # 提取文件名
        file_name = local_file_path.replace(
            '\\\\', '/').replace('\\', '/').split('/')[-1]

        # 获取上传信息
        info = self.__get_upload_info(
            dentry_uuid=dentry_uuid, file_name=file_name)
        upload_key = info["uploadKey"]
        resource_url = info["headerSignatureInfo"]["resourceUrls"][0]
        oss_headers = info["headerSignatureInfo"]["headers"]

        # 上传文件到 OSS
        upload_success = self.__upload_to_oss(
            resource_url, oss_headers, local_file_path)

        # 提交文件
        if upload_success:
            self.__commit_file(dentry_uuid, upload_key, file_name)

    def send_work_msg(self, userid_list=None, dept_id_list=None, msg=None, to_all_user=False):
        """发送工作通知消息

        向指定用户或部门发送工作通知消息（异步发送）。

        Args:
            userid_list: 用户ID列表，多个用逗号分隔，例如: "user1,user2,user3"
            dept_id_list: 部门ID列表，多个用逗号分隔，例如: "123,456,789"
            msg: 消息内容，dict格式，例如:
                 {
                     "msgtype": "text",
                     "text": {
                         "content": "测试消息"
                     }
                 }
            to_all_user: 是否发送给全部用户，默认 False

        Returns:
            dict: 发送结果，包含 task_id 等信息

        Raises:
            Exception: 发送失败时抛出异常

        Example:
            >>> app = DingtalkApp(agent_id="123", app_key="key", app_secret="secret")
            >>> app.send_work_msg(
            ...     userid_list="user1,user2",
            ...     msg={
            ...         "msgtype": "text",
            ...         "text": {
            ...             "content": "您好，这是一条测试消息"
            ...         }
            ...     }
            ... )
        """
        if not self.access_token:
            self.access_token = self.__get_access_token()

        url = "https://oapi.dingtalk.com/topapi/message/corpconversation/asyncsend_v2"
        params = {"access_token": self.access_token}

        data = {
            "agent_id": self.agent_id,
            "to_all_user": str(to_all_user).lower()
        }

        # 添加消息内容
        if msg is None:
            raise Exception("消息内容 msg 不能为空")

        # 如果 msg 是 dict，转换为 JSON 字符串
        if isinstance(msg, dict):
            data["msg"] = json.dumps(msg, ensure_ascii=False)
        else:
            data["msg"] = msg

        # 添加接收者
        if userid_list:
            if isinstance(userid_list, list):
                data["userid_list"] = ",".join(userid_list)
            else:
                data["userid_list"] = userid_list

        if dept_id_list:
            if isinstance(dept_id_list, list):
                data["dept_id_list"] = ",".join([str(d) for d in dept_id_list])
            else:
                data["dept_id_list"] = str(dept_id_list)

        # 发送请求
        response = requests.post(url, params=params, data=data).json()

        if response.get("errcode") == 0:
            return response
        else:
            raise Exception(f"发送工作消息失败: {response}")
