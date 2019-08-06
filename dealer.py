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
        url = ("https://www.twse.com.tw/fund/TWT43U?response=json&date="+date.strftime('%Y%m%d'))
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
df.columns = ['id', 'name', 'self_b', 'self_s', 'self_t', 'hedge_b', 
              'hedge_s', 'hedge_t', 'total_b', 'total_s', 'total_t']

df = df.drop(['name'], axis=1)
 
df['self_b'] = df['self_b'].str.replace(',', '')
df['self_s'] = df['self_s'].str.replace(',', '')
df['self_t'] = df['self_t'].str.replace(',', '')
df['hedge_b'] = df['hedge_b'].str.replace(',', '')
df['hedge_s'] = df['hedge_s'].str.replace(',', '')
df['hedge_t'] = df['hedge_t'].str.replace(',', '')
df['total_b'] = df['total_b'].str.replace(',', '')
df['total_s'] = df['total_s'].str.replace(',', '')
df['total_t'] = df['total_t'].str.replace(',', '')

df['self_b'] = pd.to_numeric(df['self_b'])
df['self_s'] = pd.to_numeric(df['self_s'])
df['self_t'] = pd.to_numeric(df['self_t'])
df['hedge_b'] = pd.to_numeric(df['hedge_b'])
df['hedge_s'] = pd.to_numeric(df['hedge_s'])
df['hedge_t'] = pd.to_numeric(df['hedge_t'])
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
engine.execute("CREATE TABLE dealer (id VARCHAR(255) NOT NULL,\
                                         self_b INT NULL,\
                                         self_s INT NULL,\
                                         self_t INT NULL,\
                                         hedge_b INT NULL,\
                                         hedge_s INT NULL,\
                                         hedge_t INT NULL,\
                                         total_b INT NULL,\
                                         total_s INT NULL,\
                                         total_t INT NULL,\
                                         PRIMARY KEY(id))")

# load data into the table
df.to_sql(name='dealer', con = con, if_exists='replace')
con.close()
print('finished')