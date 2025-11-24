import json
import time
from .models import Repo
from .job_executor import JobExecutor
from ..utility.time_kit import TimeKit  # 构建参数用
from config import DEBUG


class JobInstance:
    def __init__(self, job_info):
        self.log_id = None
        self.job_id = str(job_info['id'])
        self.job_params = job_info['job_params']
        self.message_robot = job_info['message_robot']
        self.job_log = True if str(job_info['job_log']) == '1' else False
        self.job_message = True if str(job_info['job_message']) == '1' else False
        self.job_type = job_info['job_type']
        self.job_template = job_info['job_template']
        self.job_name = job_info['job_name']
        self.job_on_error = int(job_info['job_on_error'])

    @staticmethod
    def __build_params(job_params):
        """
        字符串参数解析成python对象
        单层字典参数, 每个值都可以使用TimeKit辅助生成参数值
        列表参数, 每个元素为单层字典
        :param job_params: {"p1":"v1"}  or [{"p1":"v1"},{"p1":"v2"},{"p1":"v3"}]
        :return:
        """
        if job_params is None or job_params == '' or str(job_params) == 'None':
            return None
        job_params = json.loads(job_params)
        if isinstance(job_params, list):
            real_params = []
            for params in job_params:
                tmp = {}
                for k, v in params.items():
                    code_str = 'tk=TimeKit()\netl_job_params=' + str(v)
                    variables = {}
                    try:
                        exec(code_str, None, variables)
                        tmp[k] = variables['etl_job_params']
                    except Exception as e:
                        print_log(e)
                real_params.append(tmp)
        elif isinstance(job_params, dict):
            real_params = {}
            for k, v in job_params.items():
                code_str = 'tk=TimeKit()\netl_job_params=' + str(v)
                variables = {}
                try:
                    exec(code_str, None, variables)
                    real_params[k] = variables['etl_job_params']
                except Exception as e:
                    print_log(e)
        else:
            raise Exception('无效参数')
        return real_params

    def run(self):
        run_params = self.__build_params(self.job_params)  # 构建运行参数
        repo.register_job_start(self.job_id)
        if self.job_log:
            self.log_id = str(time.time_ns())  # 生成日志id
            run_params_str = '' if run_params is None else json.dumps(run_params, ensure_ascii=False)
            repo.job_log_start(self.log_id, self.job_id, run_params_str)
        # 开发模式直接抛出异常, 正式环境发送消息
        je = JobExecutor(self.job_type, self.job_template, run_params, self.job_log)
        code, msg = je.exe()
        # 根据执行结果记录日志和发送提醒
        if code == 0:
            repo.register_job_end(self.job_id)
            if self.job_log:
                repo.job_log_end(self.log_id, 3, msg)
            if self.job_message:
                run_params_str = '' if run_params is None else json.dumps(run_params, ensure_ascii=False)
                repo.msg(self.message_robot, '通知: 任务执行完成\n\n【' + self.job_name + '】\n【' + run_params_str + '】')
        else:
            repo.register_job_error(self.job_id)
            if self.job_log:
                repo.job_log_end(self.log_id, -1, msg)
            repo.admin_msg(text='ETL任务 ' + str(self.job_name) + '\n执行出错\n\n' + str(msg)[0:100])
            print_log(msg)
        # 后序job
        if DEBUG or (code == -1 and self.job_on_error == 1):
            print_log('跳过后续任务')
            return []
        # 后序任务
        follow_job_list = repo.get_follow_job(self.job_id)
        if len(follow_job_list) == 0:
            return []
        else:
            return follow_job_list


class JobManager:
    @staticmethod
    def schedule(user_job_list=None):
        job_list = repo.get_job(user_job_list)
        if len(job_list) == 0:
            print_log('----------------------------------------------------')
            print_log('没有任务')
            exit()
        # 循环执行
        while len(job_list) > 0:
            repo.register_job_pending(job_list)
            job_info = job_list.pop(0)
            job_instance = JobInstance(job_info)
            print_log('----------------------------------------------------')
            print_log(job_info)
            re = job_instance.run()
            job_list.extend(re)
