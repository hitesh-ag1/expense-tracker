import numpy as np
import pandas as pd
from datetime import datetime

def todayFilter(data):
    data = data[(data.index.year == datetime.today().year) & (data.index.month == datetime.today().month) &(data.index.day == datetime.today().day)]
    return data

def transToday(data):
    if (len(data) == 0):
        return (data.to_dict())
    trans = (data)[['amt', 'payee', 'type']]
    trans.loc[trans.type == 'Payment', 'amt'] = np.negative(trans.loc[trans.type == 'Payment', 'amt']).values
    trans = trans.groupby('payee',).amt.sum()
    trans = trans.to_dict()
    return trans

def transTodaySummarybyType(data):
    if (len(data) == 0):
        return (data.to_dict())
    da = (data)[['amt', 'type']]
    da = ((da.groupby([pd.Grouper(freq='D'), 'type']).amt.sum()).droplevel('date')).to_dict()
    return da

def transTodaySummarybyCat(data):
    if (len(data) == 0):
        return (data.to_dict())
    da = (data)[['amt', 'category', 'type']]
    da = ((da.groupby([pd.Grouper(freq='D'), 'category', 'type']).amt.sum()).droplevel('date'))
    da.loc[da.index.get_level_values('type') == 'Payment'] = np.negative(da.loc[da.index.get_level_values('type') == 'Payment'])
    da = da.droplevel('type')
    da = da.to_dict()
    return da
    # summary = data.groupby([pd.Grouper(freq='D'), 'category', 'type']).amt.sum()
    # day_summary = todayMultiIndexFilter(summary)
    # day_summary = day_summary.droplevel('datetime')
    # day_summary.loc[day_summary.index.get_level_values('type') == 'Payment'] = np.negative(day_summary.loc[day_summary.index.get_level_values('type') == 'Payment'])
    # day_summary = day_summary.droplevel('type')
    # day_summary = day_summary.to_dict()
