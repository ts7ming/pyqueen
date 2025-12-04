# crud/etl_job.py
from sqlalchemy.orm import Session
from models import EtlJob
from .base import CRUDBase

class CRUDEtlJob(CRUDBase[EtlJob, EtlJobCreate, EtlJobUpdate]):
    def get_by_name(self, db: Session, name: str) -> EtlJob | None:
        return db.query(self.model).filter(self.model.job_name == name).first()

    def get_enabled_jobs(self, db: Session) -> list[EtlJob]:
        return db.query(self.model).filter(self.model.job_status == 1).all()

    def update_execution_status(self, db: Session, job_id: int, status: int) -> EtlJob | None:
        job = self.get(db, job_id)
        if job:
            job.execution_status = status
            db.commit()
            db.refresh(job)
        return job

# 实例化（可直接导入使用）
etl_job = CRUDEtlJob(EtlJob)