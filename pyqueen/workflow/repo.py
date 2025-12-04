from sqlalchemy.orm import Session
from models2 import EtlJob
from .base import CRUDBase
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 示例 URL（实际应从配置读取）
DATABASE_URL = "mysql+pymysql://user:pass@localhost/mydb?charset=utf8mb4"
# DATABASE_URL = "mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """FastAPI 依赖或手动使用"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


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