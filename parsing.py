import pandas as pd
import re
import numpy as np
from datetime import datetime
from dateutil import parser
import datefinder

#type0 - 'CARD TRANSACTION ALERT'
def key_extractor_0(data):
    card = list(data['msg'])
    card_subjs = list(data['subject'])

    trans_amts = pd.Series(card).str.findall('SGD [0-9.]+').str.join(',').str.replace(' ', '') + pd.Series(card).str.findall('SGD[0-9.]+').str.join(',')
    trans_amts=trans_amts.str.replace('SGD','')
    trans_amts = trans_amts.astype(float)

    card_df = pd.DataFrame([card_subjs, card, trans_amts]).T
    card_df.columns=['subject','msg', 'amt'] 

    datetime = []
    for x in range(len(card_df)):
        date = list(datefinder.find_dates(card_df.msg[x], strict=True))[0]
        datetime.append(date)
        
    card_df['datetime'] = datetime

    payee = card_df['msg'].str.findall('\(-[0-9]+\) at (.*?).If').str.join(',')
    card_df['payee'] = payee
    return card_df

#type1 - 'Successful NETS Payment'
def key_extractor_1(type_1):
    parsed_date_string = ((type_1.msg.str.findall('Date of Transfer(.*)').str.join('') + type_1.msg.str.findall('Time of Transfer(.*)').str.join('')).str.replace('  ', '').str.replace(': ',' '))

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_1['datetime'] = datetime

    type_1['amt'] = type_1['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','').astype(float)

    type_1['payee'] = type_1.msg.str.findall('Merchant Name (.*)').str.join('') .str.replace('  ', '').str.replace(': ',' ')

    return type_1

#type2 - 'You have sent money via OCBC Pay Anyone'
def key_extractor_2(type_2):
    parsed_date_string = ((type_2.msg.str.findall('Date of transfer(.*?)Time').str.join('') + type_2.msg.str.findall('Time of transfer(.*?)Ref').str.join('')).str.replace('  ', '').str.replace(': ',' ')).str.replace('.',':').str.strip()

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_2['datetime'] = datetime

    type_2['amt'] = (type_2['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','') + type_2['msg'].str.findall('SGD[0-9.]+').str.join(',').str.replace('SGD','')).astype(float)

    type_2['payee'] = type_2.msg.str.findall('PayNow name (.*)Amount').str.join('') .str.replace('  ', '').str.replace(': ',' ') + type_2.msg.str.findall('Customer(.*) has received').str.join('') .str.replace('  ', '').str.replace(': ',' ').str.strip()

    return type_2

#type3 - 'You have successfully sent money via OCBC Pay Anyone'
def key_extractor_3(type_3):
    parsed_date_string = type_3.msg.str.findall('Date Sent(.*?)Ref').str.join('').str.replace('  ', '').str.replace(': ',' ').str.strip()

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_3['datetime'] = datetime

    type_3['amt'] = (type_3['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','') + type_3['msg'].str.findall('SGD[0-9.]+').str.join(',').str.replace('SGD','')).astype(float)

    type_3['payee'] = type_3.msg.str.findall('PayNow name (.*)Amount').str.join('') .str.replace('  ', '').str.replace(': ',' ')

    return type_3

#type_4 - SPECIAL CASE - 'You have sent money to'
def key_extractor_4(type_4):
    parsed_date_string = ((type_4.msg.str.findall('Date of Transfer(.*?)\n').str.join('') + type_4.msg.str.findall('Time of Transfer(.*?)\n').str.join('')).str.replace('  ', '').str.replace(': ',' ')).str.replace('.',':').str.strip() + ((type_4.msg.str.findall('Date of Transfer(.*?)Time').str.join('') + type_4.msg.str.findall('Time of Transfer(.*?)Amount').str.join('')).str.replace('  ', '').str.replace(': ',' ')).str.replace('.',':').str.strip()

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_4['datetime'] = datetime
    type_4['amt'] = (type_4['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','') + type_4['msg'].str.findall('SGD[0-9.]+').str.join(',').str.replace('SGD','')).astype(float)
    type_4['payee'] = type_4.msg.str.findall('sent money to (.*) using').str.join('') .str.replace('  ', '').str.replace(': ',' ')

    return type_4

#type5 - 'OCBC Alert: Successful ATM QR Withdrawal'
def key_extractor_5(type_5):
    parsed_date_string = type_5.msg.str.findall('Payment Date(.*?)Amount').str.join('').str.replace('  ', '').str.replace(': ',' ').str.strip()

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_5['datetime'] = datetime

    type_5['amt'] = (type_5['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','') + type_5['msg'].str.findall('SGD[0-9.]+').str.join(',').str.replace('SGD','')).astype(float)

    type_5['payee'] = type_5.msg.str.findall('To (.*)eNets').str.join('') .str.replace('  ', '').str.replace(':',' ').str.strip()

    return type_5


#type6 - 'You have sent money via PayNow'
def key_extractor_6(type_6):
    parsed_date_string = ((type_6.msg.str.findall('Date of Transfer(.*?)Time').str.join('') + type_6.msg.str.findall('Time of Transfer(.*?)Amount').str.join('')).str.replace('  ', '').str.replace(': ',' ')).str.replace('.',':').str.strip()

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_6['datetime'] = datetime

    type_6['amt'] = (type_6['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','') + type_6['msg'].str.findall('SGD[0-9.]+').str.join(',').str.replace('SGD','')).astype(float)

    type_6['payee'] = type_6.msg.str.findall('to (.*) using').str.join('') .str.replace('  ', '').str.replace(':',' ').str.strip()

    return type_6

#type7 - 'OCBC Alert: Successful ATM QR Withdrawal'
def key_extractor_7(type_7):
    parsed_date_string = (type_7.msg.str.findall('OCBC ATM at (.*?). Quest').str.join(''))

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_7['datetime'] = datetime

    type_7['amt'] = type_7.msg.str.findall('OCBC: S(.*?) was').str.join('').str.replace('[$,]','').astype(float)

    type_7['payee'] = 'ATM Withdrawal'

    return type_7


#type8 - 'OCBC Alert: You have successfully sent money via PayNow'
def key_extractor_8(type_8):
    parsed_date_string = type_8.msg.str.findall('Transfer Date(.*?)Amount').str.join('').str.replace('  ', '').str.replace(': ',' ').str.strip()

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_8['datetime'] = datetime

    type_8['amt'] = (type_8['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','') + type_8['msg'].str.findall('SGD[0-9.]+').str.join(',').str.replace('SGD','')).astype(float)

    type_8['payee'] = type_8.msg.str.findall('to (.*) using').str.join('') .str.replace('  ', '').str.replace(':',' ').str.strip()

    return type_8


#type9 - 'We have received your funds transfer request'
def key_extractor_9(type_9):
    parsed_date_string = ((type_9.msg.str.findall('Date of Transfer(.*?)Time').str.join('') + type_9.msg.str.findall('Time of Transfer(.*?)Amount').str.join('')).str.replace('  ', '').str.replace(':',' ')).str.replace('.',':').str.strip()

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_9['datetime'] = datetime

    type_9['amt'] = (type_9['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','') + type_9['msg'].str.findall('SGD[0-9.]+').str.join(',').str.replace('SGD','')).astype(float)

    type_9['payee'] = type_9.msg.str.findall('To account(.*)Ref').str.join('') .str.replace('  ', '').str.replace(':',' ').str.strip()

    return type_9


#type10 - 'OCBC Alert: Deposit on your account'
def key_extractor_10(type_10):
    parsed_date_string = ((type_10.msg.str.findall('Date of deposit(.*?)Time').str.join('') + type_10.msg.str.findall('Time of deposit(.*?)Amount').str.join('')).str.replace('  ', '').str.replace(': ',' ')).str.strip()

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_10['datetime'] = datetime

    type_10['amt'] = (type_10['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','') + type_10['msg'].str.findall('SGD[0-9.]+').str.join(',').str.replace('SGD','')).astype(float)

    type_10['payee'] = type_10.msg.str.findall('from(.*)For').str.join('') .str.replace('  ', '').str.replace(':',' ').str.strip()

    return type_10

#type11 - 'OCBC Alert: Deposit on your account'
def key_extractor_11(type_10):
    parsed_date_string = ((type_10.msg.str.findall('Date of deposit(.*?)Time').str.join('') + type_10.msg.str.findall('Time of deposit(.*?)Amount').str.join('')).str.replace('  ', '').str.replace(': ',' ')).str.strip()

    datetime = []
    for i in parsed_date_string:
        datetime.append(list(datefinder.find_dates(i))[0])

    type_10['datetime'] = datetime

    type_10['amt'] = (type_10['msg'].str.findall('SGD [0-9.]+').str.join(',').str.replace('SGD ','') + type_10['msg'].str.findall('SGD[0-9.]+').str.join(',').str.replace('SGD','')).astype(float)

    type_10['payee'] = type_10.msg.str.findall('from(.*)For').str.join('') .str.replace('  ', '').str.replace(':',' ').str.strip()

    return type_10