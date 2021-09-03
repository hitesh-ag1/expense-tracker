import pandas as pd
import numpy as np

shops = ['Successful NETS Payment', 'You have sent money to', 'Successful eNETS payment', ]

friends = ['You have sent money via OCBC Pay Anyone', 'You have successfully sent money via OCBC Pay Anyone', 'OCBC Alert: You have successfully sent money via PayNow', 'You have sent money via PayNow', 'OCBC Alert: Deposit on your account', 'OCBC Alert: Deposit in your account']

work_related = ['We have received your funds transfer request']

categories = {
'Restaurant': ["STUFF'D JURONG POINT SING", 'Deliveroo', '7-ELEVEN -NTU 35 NANYANG', 'Foodpanda Singapore Singa', 'WHOLLY GREENS PTE LTD SIN', 'THE CROWDED BOWL @ NTU', 'WECOME PTE LTD', 'THE SOUP SPOON PTE LTD', 'ZI CHAR', 'VISIONE NTU PTE.LTD.', 'ARASAA ANANDA FOODS PTE. LTD.', 'PITA BAKERY','LE TACH PTE LTD', 'PITA BAKERY PTE. LTD.', 'NASTY COOKIE', 'BAKEPOINT-MM', 'MARCHE'],
'Food': ['INDIAN', 'WESTERN & ITALIAN CUISINE', 'DELHI EXPRESS PTE.LTD.', 'PASTA EXPRESS', 'JAPANESE', 'WESTERN', 'DRINK STALL', 'DRINKS', 'COFFEE / TEA', 'FRUITS JUICE', 'FRUITS', 'BEVERAGES','LE TACH PTE LTD', 'Can 9', 'BRIYANI POINT', 'SUBWAY', 'BAKERY CUISINE', 'KOUFU'],
'Investment': ['www.coinhako.com INTERNET'],
'Transport': ['Grab', 'COMFORT TAXI', 'BUS/MRT', 'Gojek'],
'Grocery' : ['FAIRPRICE', 'PRIME SPKMT', 'PRIME SPKMT', 'GIANT', 'SKT MART PTE. LTD.'],
'Telco' : ['SINGTEL PREPAID HI!ACC Si', 'Singapore Telecommunications Limited'],
'Entertainment' : ['CATHAY-JEM SINGAPORE SG', 'AMZNPRIME'],
'University Payment' : ['NTU - SINGPORE SINGAPORE', 'NANYANG TECHNOLOGICAL UNIVERSITY'],
'Healthcare' : ['FULLERTON HEALTHC', 'MEDICAL'],
'Personal Care' : ['HAIR DESTINATION'],
'Education' : ['ULTRA SUPPLIES', 'BOOKLINK PTE LTD', 'GOOGLE*CLOUD'],
'ATM Withdrawal': ['ATM Withdrawal']
}

def assign_labels(payments_data):

    cat = np.zeros(len(payments_data), dtype=object)

    for label in categories.keys():
        cat[list(payments_data[payments_data.payee.str.contains('|'.join([f'(?i){payee_label}' for payee_label in categories[label]]), case=False)].index)] = label

    cat[np.where(cat == 0)] = 'Uncategorised'

    payments_data['category'] = cat

    cat[payments_data[payments_data.category == 'Uncategorised'][(payments_data[payments_data.category == 'Uncategorised'].subject.str.contains('|'.join([f'(?i){payee_label}' for payee_label in friends]))) & (payments_data[payments_data.category == 'Uncategorised'].payee != '')].index] = 'Friends'
    payments_data['category'] = cat
    cat[payments_data[payments_data.category == 'Uncategorised'][(payments_data[payments_data.category == 'Uncategorised'].subject.str.contains('|'.join([f'(?i){payee_label}' for payee_label in friends]))) & (payments_data[payments_data.category == 'Uncategorised'].payee == '')].index] = 'General Deposit'
    payments_data['category'] = cat
    payments_data.index = payments_data.datetime
    payments_data.drop(['datetime'], axis=1, inplace=True)
    
    deposits = ['OCBC Alert: Deposit on your account', 'OCBC Alert: Deposit in your account']
    payments_data['type'] = None
    payments_data.loc[payments_data.subject.isin(deposits), 'type'] = 'Deposit'
    payments_data.loc[~payments_data.subject.isin(deposits), 'type'] = 'Payment'

def labelFriends(data):
    filt = data[data.category == 'Friends'].copy()
    
    for i in range(len(filt)):
        print(filt.payee[i], filt.amt[i], filt.index[i].time())
        cat = ' - '+input()
        (filt.loc[filt.index[i], 'category']) = filt.category[i]+ cat

    return filt