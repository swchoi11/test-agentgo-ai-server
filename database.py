import os
from utils import get_secret
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Session
from google.cloud.sql.connector import Connector, IPTypes

db_user = get_secret("DB_USER")
db_password = get_secret("DB_PASSWORD")
db_name = get_secret("DB_NAME")
region = get_secret("REGION")
instance_name = get_secret("INSTANCE_NAME")

connector = Connector()

def getconn():
    conn = connector.connect(
        f"agentgo-studio:{region}:{instance_name}",
        "pg8000",
        user=db_user,
        password=db_password,
        db=db_name,
        ip_type=IPTypes.PRIVATE
    )
    return conn

engine = create_engine(
    "postgresql+pg8000://",
    creator=getconn
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Base(DeclarativeBase):
    pass

class Test(Base):
    __tablename__ = "test"
    id = Column(Integer, primary_key=True, index=True)
    user_name = Column(String)
    user_input = Column(Integer)
    vm_output = Column(String)

def get_db():
    db=SessionLocal()
    try:
        yield db
    finally:
        db.close()

def update_record(db_id: int, vm_output: str):
    db = SessionLocal()
    try:
        record = db.query(Test).filter(Test.id==db_id).first()
        if record:
            record.vm_output = vm_output
            db.commit()
            return True
    except Exception as e:
        db.rollback()
        print(e)
        raise e
    finally:
        db.close()