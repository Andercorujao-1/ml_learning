
from datetime import datetime
from sqlalchemy import Column, Integer, String, JSON, DateTime, Index
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class RequestsHistory(Base):
    __tablename__ = "requests_history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    apiName = Column(String, nullable=False)
    endpointName = Column(String, nullable=False)
    requestDate = Column(DateTime, nullable=False, default=datetime.now())
    info = Column(JSON)

    __table_args__ = (
        Index("ix_requests_history_api_endpoint_datetime", "apiName", "endpointName", "dateTime"),
    )