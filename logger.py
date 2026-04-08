from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime
from db import Base, engine, SessionLocal

# -----------------------------
# LOG TABLE MODEL
# -----------------------------
class ETLLog(Base):
    __tablename__ = "etl_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String)
    raw_count = Column(Integer)
    clean_count = Column(Integer)
    agg_count = Column(Integer)
    timestamp = Column(DateTime, default=datetime.utcnow)


# -----------------------------
# CREATE TABLE
# -----------------------------
def create_log_table():
    Base.metadata.create_all(bind=engine)


# -----------------------------
# INSERT LOG
# -----------------------------
def log_etl(file_name, raw_count, clean_count, agg_count):

    session = SessionLocal()

    log = ETLLog(
        file_name=file_name,
        raw_count=raw_count,
        clean_count=clean_count,
        agg_count=agg_count
    )

    session.add(log)
    session.commit()
    session.close()