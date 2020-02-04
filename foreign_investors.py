'''
web scraping reference:
https://medium.com/renee0918/python-%E7%88%AC%E5%8F%96%E5%80%8B%E8%82%A1%E6%AD%B7%E5%B9%B4%E8%82%A1%E5%83%B9%E8%B3%87%E8%A8%8A-b6bc594c8a95
'''
# import package
from datetime import date,timedelta
from urllib.request import urlopen
from dateutil import rrule
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import numpy as np
import json
import time

def craw_one_day(date):
    try:
        url = ("https://www.twse.com.tw/fund/TWT38U?response=json&date="+date.strftime('%Y%m%d'))
        data = json.loads(urlopen(url).read())
        return pd.DataFrame(data['data'],columns=data['fields'])
    except:
        print('KeyError', date.strftime('%Y-%m-%d'))

def craw_stock(start_day):
    b_day = date(*[int(x) for x in start_day.split('-')])
    now = datetime.datetime.now().strftime("%Y-%m-%d")         # 取得現在時間
    e_day = date(*[int(x) for x in now.split('-')])

    result = pd.DataFrame()
    # change rrule.MONTHLY to rrule.DAILY
    for dt in rrule.rrule(rrule.DAILY, dtstart=b_day, until=e_day):
        result = pd.concat([result,craw_one_day(dt)],ignore_index=True)
        time.sleep(5000.0/1000.0);
    
    return result

# 三大法人數據不是每天都有，遇到沒有數據的天數會產生 KeyError: 'data'
# 用try, except解決?

df = craw_stock('2019-08-01')
df.columns = ['NaN', 'id', 'name', 'notself_b', 'notself_s', 'notself_t', 'self_b', 'self_s', 'self_t', 'total_b', 'total_s', 'total_t']

df = df.drop(['NaN', 'name'], axis=1)
 
df['notself_b'] = df['notself_b'].str.replace(',', '')
df['notself_s'] = df['notself_s'].str.replace(',', '')
df['notself_t'] = df['notself_t'].str.replace(',', '')
df['self_b'] = df['self_b'].str.replace(',', '')
df['self_s'] = df['self_s'].str.replace(',', '')
df['self_t'] = df['self_t'].str.replace(',', '')
df['total_b'] = df['total_b'].str.replace(',', '')
df['total_s'] = df['total_s'].str.replace(',', '')
df['total_t'] = df['total_t'].str.replace(',', '')

df['notself_b'] = pd.to_numeric(df['notself_b'])
df['notself_s'] = pd.to_numeric(df['notself_s'])
df['notself_t'] = pd.to_numeric(df['notself_t'])
df['self_b'] = pd.to_numeric(df['self_b'])
df['self_s'] = pd.to_numeric(df['self_s'])
df['self_t'] = pd.to_numeric(df['self_t'])
df['total_b'] = pd.to_numeric(df['total_b'])
df['total_s'] = pd.to_numeric(df['total_s'])
df['total_t'] = pd.to_numeric(df['total_t'])

# Can pymysql do this?
from sqlalchemy import create_engine

engine = create_engine("mysql://root:py9m9s34@localhost/stocks")
con = engine.connect()
# Create table in mysql
# How to create multiple tables?
'''
df = 'df20328417'
engine.execute("CREATE TABLE *s (......)" % (df))
is work
but df.to_sql failed
'''

'''
df = ['df20190711_16', 'd20190711_17']
engine.execute("CREATE TABLE %s (.....)" % (df))
not work
'''
# try to def a function?
engine.execute("CREATE TABLE foreign_investors (id VARCHAR(255) NOT NULL,\
                                         notself_b INT NULL,\
                                         notself_s INT NULL,\
                                         notself_t INT NULL,\
                                         self_b INT NULL,\
                                         self_s INT NULL,\
                                         self_t INT NULL,\
                                         total_b INT NULL,\
                                         total_s INT NULL,\
                                         total_t INT NULL,\
                                         PRIMARY KEY(id))")

# load data into the table
df.to_sql(name='foreign_investors', con = con, if_exists='replace')
con.close()
print('finished')
