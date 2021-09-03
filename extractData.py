from decouple import config
from imapclient import IMAPClient
import mailparser
from bs4 import BeautifulSoup
import pickle
import pandas as pd
import re
import numpy as np
from datetime import datetime
from dateutil import parser
import datefinder
import parsing

class dataExtraction:
    """Class that extracts all previous transactions for the user"""
    
    def __init__(self, email, psswd):
        self.email = email
        self.passwd = psswd
        self.card_payment = []
        self.pay_anyone = []
        self.all_payments = []
        self.parsed_data = []

    def extract_emails(self, date, folder):
        server = IMAPClient('imap-mail.outlook.com', ssl=True)
        server.login(self.email, self.passwd)
        server.select_folder(folder, readonly=True)

        UID1 = server.search(['FROM','noreply@notify.ocbc.com','SINCE', date])
        
        for email in UID1:
            rawMSG = server.fetch([email], ['BODY[]'])
            msg = mailparser.parse_from_bytes(rawMSG[email][b'BODY[]'])
            soup = BeautifulSoup(msg.body, 'html.parser')
            eg = soup.get_text().strip()
            self.card_payment.append({'subject': msg.headers['Subject'], 'desc': ''.join([text.strip() for text in eg.split('\r')])})

        self.card_payment = (pd.DataFrame(self.card_payment)[pd.DataFrame(self.card_payment).subject == 'CARD TRANSACTION ALERT']).to_dict('records')
        
        UID2 = server.search(['FROM','notifications@ocbc.com','SINCE', date])
        for email in UID2:
            rawMSG = server.fetch([email], ['BODY[]'])
            msg = mailparser.parse_from_bytes(rawMSG[email][b'BODY[]'])
            soup = BeautifulSoup(msg.body, 'html.parser')
            eg = soup.get_text().strip()
            self.pay_anyone.append({'subject': msg.headers['Subject'], 'msg': ''.join([text.strip() for text in eg.split('\r')])})

        self.card_payment = pd.DataFrame(self.card_payment)
        self.card_payment.columns = ['subject', 'msg']
        self.pay_anyone = pd.DataFrame(self.pay_anyone)
        self.all_payments = pd.concat([self.card_payment, self.pay_anyone])
        self.all_payments.index = np.arange(len(self.all_payments))
        
    def parse_key_values(self):
        key_data = []
        subjects = ['CARD TRANSACTION ALERT', 'Successful NETS Payment', 'You have sent money via OCBC Pay Anyone', 'You have successfully sent money via OCBC Pay Anyone', 'You have sent money to','Successful eNETS payment','You have sent money via PayNow' ,'OCBC Alert: Successful ATM QR Withdrawal', 'OCBC Alert: You have successfully sent money via PayNow', 'We have received your funds transfer request', 'OCBC Alert: Deposit on your account', 'OCBC Alert: Deposit in your account']
        
        # print(set(subjects))
        # print(set((np.append(self.all_payments.subject.unique()[~pd.Series(self.all_payments.subject.unique()).str.contains('You have sent money to')],np.array(['You have sent money to'])))))
        
        # print(set(subjects) == set((np.append(self.all_payments.subject.unique()[~pd.Series(self.all_payments.subject.unique()).str.contains('You have sent money to')],np.array(['You have sent money to'])))))
        for i in range(len(subjects)):
            if(i==4):
                exec('type_'+str(i)+'= self.all_payments[self.all_payments.subject.str.findall(subjects['+str(i)+']).str.join(\',\').str.len() !=0]')
            else:
                exec('type_'+str(i)+'= self.all_payments[self.all_payments.subject == subjects['+str(i)+']]')

            exec('key_data.append(parsing.key_extractor_'+str(i)+'(type_'+str(i)+'))')
            
        self.parsed_data = pd.concat(key_data)
        self.parsed_data.index = np.arange(len(self.parsed_data))
        self.parsed_data.payee = self.parsed_data.payee.str.strip()  
        
# data = dataExtraction(config('EMAIL'), config('PSSWD'))
# data.extract_emails()
