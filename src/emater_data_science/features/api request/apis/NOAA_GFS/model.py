from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class ForecastData(Base):
    __tablename__ = 'forecast_data'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)  # When the forecast is for
    temperature = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    wind_speed = Column(Float, nullable=True)
    location = Column(String, nullable=False)

# Connect to the database (replace with your actual DB path or connection string)
engine = create_engine('yourdatabase')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)