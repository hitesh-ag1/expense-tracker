from email.policy import HTTP
from sqlalchemy.sql.expression import null, true
from fastapi import FastAPI, Depends
from pydantic import BaseModel
import datetime as dt
from datetime import datetime, date, timedelta
from sqlalchemy import func
from typing import Optional, List
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import DateTime, Column, Float, String, Integer
from sqlalchemy.sql import case
import extractData
import numpy as np
import pandas as pd
from labelData import assign_labels, labelFriends
from matplotlib import pyplot as plt
import os
from fastapi.encoders import jsonable_encoder
from todayFuncs import transToday, transTodaySummarybyCat, transTodaySummarybyType
from dotenv import load_dotenv
load_dotenv()

app = FastAPI()

security=HTTPBasic()

# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
# POSTGRES_SERVER = os.getenv("POSTGRES_SERVER","localhost")
# POSTGRES_PORT = os.getenv("POSTGRES_PORT",5432)
# POSTGRES_DB = os.getenv("POSTGRES_DB","tdd")

# SQLALCHEMY_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL_ALCHEMY")
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db"
# engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True, future=True, connect_args={'check_same_thread': False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class DBTransaction(Base):
    __tablename__ = 'transaction'
    id = Column(Integer, primary_key=True, index=True)
    date = Column(DateTime)
    subject = Column(String)
    msg = Column(String)
    amt = Column(Float)
    payee = Column(String)
    category = Column(String)
    type = Column(String)
    label = Column(String)

Base.metadata.create_all(bind=engine)

class Transaction(BaseModel):
    date: Optional[datetime]
    subject: Optional[str]
    msg: Optional[str]
    amt: Optional[float]
    payee: Optional[str]
    category: Optional[str]
    type: Optional[str]
    label: Optional[str]
    
    class Config:
        orm_mode=True

class AccountDetails(BaseModel):
    email: Optional[str]
    psswd: Optional[str]

    class Config:
        orm_mode=True


def get_past_account_trans(db: Session, email: str, psswd: str, date: str, folder: str):
    data = extractData.dataExtraction(email, psswd)
    data.extract_emails(date, folder)
    # data.extract_emails('01-Jun-2021', 'StudBud')
    data.parse_key_values()
    temp2 = data.parsed_data.copy()
    assign_labels(temp2)
    temp2.sort_index(ascending=False, inplace=True)
    temp2['label'] = None
    temp2.loc[pd.Index(temp2.category == 'ATM Withdrawal'), 'label'] = 'Special Case'
    # temp2 = labelFriends(temp2)
    temp2['date'] = list(temp2.index)
    temp2 = temp2.to_dict('records')
    for trans in temp2:
        db_trans = create_transaction(db, Transaction(**trans))     
    return db.query(DBTransaction).all()


def get_all_transactions(db: Session):
    return db.query(DBTransaction).all()

def get_transactions_by_date(db: Session, start_date: dt.date, end_date: dt.date):
    return db.query(DBTransaction).filter(func.date(DBTransaction.date) >= start_date ).filter(func.date(DBTransaction.date) <= end_date).all()

def get_transactions_today_df(db: Session):
    inp_date = str(date.today() - timedelta(1))
    trans_today = jsonable_encoder(get_transactions_by_date(db, inp_date, inp_date))
    trans_today = pd.DataFrame(trans_today)
    trans_today.date = pd.to_datetime(trans_today.date)
    trans_today.index = trans_today.date
    return trans_today

def create_transaction(db: Session, transaction: Transaction):
    db_trans = DBTransaction(**transaction.dict())
    db.add(db_trans)
    db.commit()
    db.refresh(db_trans)
    return db_trans

def delete_transaction(db: Session, trans_id: int):
    db.query(DBTransaction).where(DBTransaction.id == trans_id).delete()
    db.commit()

def delete_all(db: Session):
    db.query(DBTransaction).delete()
    db.commit()

def update_db_category(db: Session, transactions: dict):
    db.query(DBTransaction).filter(
        DBTransaction.date.in_(transactions)
    ).update({
        DBTransaction.category: case(
            transactions,
            value=DBTransaction.date
        )
    }, synchronize_session=False)
    db.commit()
    return (True)

@app.post('/transaction/', response_model = Transaction)
async def create_transaction_view(transaction: Transaction, db: Session = Depends(get_db)):
    db_transaction = create_transaction(db, transaction)
    return db_transaction

@app.get('/add_past_transactions_to_db/' ,response_model = List[Transaction])
def get_past_account_trans_from_email_view(date: str, folder: str , db: Session = Depends(get_db), credentials: HTTPBasicCredentials = Depends(security)):
    return get_past_account_trans(db, credentials.username, credentials.password, date, folder)

@app.get('/all_transactions/', response_model = List[Transaction])
def get_all_transactions_view(db: Session = Depends(get_db)):
    return get_all_transactions(db)

@app.get('/transaction_by_date/')
def get_transaction_by_date_view(start_date: dt.date, end_date:dt.date , db: Session=Depends(get_db)):
    return get_transactions_by_date(db, start_date, end_date)

@app.get('/transactions_today/')
def get_transactions_today(db: Session=Depends(get_db)):
    df = get_transactions_today_df(db)
    return transToday(df)

@app.get('/transactions_today_inflow_outflow/')
def get_transactions_today_in_out(db: Session=Depends(get_db)):
    df = get_transactions_today_df(db)
    return transTodaySummarybyType(df)

@app.get('/transactions_today_cat_summary/')
def get_transactions_today_cat_summary(db: Session=Depends(get_db)):
    df = get_transactions_today_df(db)
    return transTodaySummarybyCat(df)

@app.get('/friends_today/')
def get_transactions_with_friends_today(db: Session=Depends(get_db)):
    # inp_date = str((date.today() - timedelta(6)).isoformat())
    trans_today = get_transactions_today_df(db)
    return((trans_today[trans_today.category == 'Friends'][['amt', 'date', 'payee', 'type', 'category']]).to_dict("records"))

@app.put('/update_category_friends_today/')
def put_transactions_with_friends_today(cat: List[str], db: Session=Depends(get_db)):
    # inp_date = str((date.today() - timedelta(6)).isoformat())
    trans_today = get_transactions_today_df(db)
    cat_today = trans_today.loc[trans_today.category == 'Friends', 'category']
    trans_today.loc[trans_today.category == 'Friends', 'category'] = ([i+ ' - ' +j for i, j in zip(cat_today, cat)])
    trans_today = trans_today[['date', 'category']]
    trans_today.index = trans_today.date
    trans_today.drop(['date'], axis=1, inplace=True)
    trans_today = (trans_today.to_dict()['category'])
    return update_db_category(db, trans_today)
    

@app.delete('/transaction/{trans_id}')
def delete_transaction_view(trans_id: int, db: Session = Depends(get_db)):
    delete_transaction(db, trans_id)

@app.delete('/all_transactions/')
def delete_all_transaction_view(db: Session = Depends(get_db)):
    delete_all(db)

@app.get('/')
async def root():
    return {'message': 'Hello World!'}
