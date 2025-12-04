from sqlalchemy import (
    Column, Integer, String, DateTime, Text,
    create_engine
)
from sqlalchemy.orm import DeclarativeBase

Base = DeclarativeBase()

class EtlJob(Base):
    __tablename__ = 'etl_job'

    id = Column(Integer, primary_key=True, comment='job_id')
    job_name = Column(String(100), comment='名称')
    job_type = Column(String(50), comment='类型: py,sync,check')
    job_template = Column(String(200), comment='作业模板: etl_job_sync/py/check (多个任务逗号分隔)')
    job_params = Column(Text, comment='作业参数 (和实际脚本/SQL参数保持一致)(支持tk对象计算日期时间)')
    execution_status = Column(Integer, comment='执行状态. 0:待执行,1:排队中,2:执行中,3:执行完成;-1执行出错;99:立即执行')
    job_status = Column(Integer, comment='作业状态: 0:停用, 1:启用')
    last_execution_time = Column(DateTime, comment='上次执行时间')
    job_depend = Column(String(100), comment='作业依赖, 前序job_id (前序作业完成后自动执行本作业)')
    job_on_error = Column(Integer, comment='作业出错处理. 0: 继续执行后序job, 1: 停止执行后序job')
    job_log = Column(Integer, comment='作业执行日志 0: 不记录, 1: 记录')
    job_message = Column(Integer, comment='作业通知 0:成功不通知, 1: 成功时通知 (失败通知admin)')
    message_robot = Column(String(100), comment='报错机器人(为空不通知)')
    job_schedule_minute = Column(String(120), comment='作业计划-分钟')
    job_schedule_hour = Column(String(100), comment='作业计划-小时')
    job_schedule_day = Column(String(100), comment='作业计划-日期')
    job_schedule_week = Column(String(20), comment='作业计划-星期')
    job_schedule_month = Column(String(50), comment='作业计划-月份')
    job_remark = Column(String(50), comment='作业备注')
    job_executor = Column(Integer, comment='任务执行服务器')
    retry_count = Column(Integer, comment='失败重试次数')
    error_count = Column(Integer, comment='失败次数')