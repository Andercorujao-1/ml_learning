# logging_table_model.py
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import DeclarativeBase
from pydantic import BaseModel


class Base(DeclarativeBase):
    pass


class LoggingTableModel(BaseModel):
    level: str
    message: str
    variablesJson: str = ""


class LoggingTable(Base):
    __tablename__ = "generalLogging"
    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    level = Column(String(50))
    message = Column(String)
    variablesJson = Column(Text)

    @classmethod
    def fromModel(cls, data: "LoggingTableModel") -> "LoggingTable":
        return cls(
            time=datetime.now(timezone.utc),
            level=data.level,
            message=data.message,
            variablesJson=data.variablesJson,
        )
