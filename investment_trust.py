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
        url = ("https://www.twse.com.tw/fund/TWT44U?response=json&date="+date.strftime('%Y%m%d'))
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
df.columns = ['NaN', 'id', 'name', 'buy', 'sell', 'total']

df = df.drop(['NaN', 'name'], axis=1)
 
df['buy'] = df['buy'].str.replace(',', '')
df['sell'] = df['sell'].str.replace(',', '')
df['total'] = df['total'].str.replace(',', '')

df['buy'] = pd.to_numeric(df['buy'])
df['sell'] = pd.to_numeric(df['sell'])
df['total'] = pd.to_numeric(df['total'])

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
engine.execute("CREATE TABLE investment_trust (id VARCHAR(255) NOT NULL,\
                                         buy INT NULL,\
                                         sell INT NULL,\
                                         total INT NULL,\
                                         PRIMARY KEY(id))")

# load data into the table
df.to_sql(name='investment_trust', con = con, if_exists='replace')
con.close()
print('finished')