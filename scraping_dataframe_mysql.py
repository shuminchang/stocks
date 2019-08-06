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

# define a function to get url
def craw_one_month(stock_number,date):
    # get url
    # https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&date=20190712&stockNo=2330
    url = (
        "http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date="+
        date.strftime('%Y%m%d')+
        "&stockNo="+
        str(stock_number)
    )
    # the content in the url is already json type, so don't need to encoding
    # urlopen url, and load to json
    data = json.loads(urlopen(url).read())
    # get 'data' as table content in data, 'fields' as name of the columns
    return pd.DataFrame(data['data'],columns=data['fields'])

def craw_stock(stock_number, start_month):
    # define beginning time
    # without date() will give you a list(string)
    b_month = date(*[int(x) for x in start_month.split('-')])
    # get only "Y-m-d" right now
    # remove strftime will give you "Y-m-d h:m:s.xxxxx"
    now = datetime.datetime.now().strftime("%Y-%m-%d")         # 取得現在時間
    # define ending time
    e_month = date(*[int(x) for x in now.split('-')])
    
    # create an empty dataframe
    # What happened if we don't put on an empty dataframe
    '''
    if we don't use empty dataframe, we will have to concat craw_one_month and 
    craw_one_month + 1(means date+1), which is complicated.
    '''
    result = pd.DataFrame()
    # create multiple date from b_month to e_month
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=b_month, until=e_month):
        # concat empty dataframe with crawed one and ignore the index
        result = pd.concat([result,craw_one_month(stock_number,dt)],ignore_index=True)
        # Stop for 5 sec after picking up one set of data
        time.sleep(5000.0/1000.0);
    '''
    min time is not 2 sec, it seems the larger the data, the longer the 
    waiting time.
    '''
    # min is 5 sec
    return result

# 20190706
# how to scrap more than one stock? Should modify the function?
df = craw_stock(2317,"2019-01-01")
df.columns = ['date', 'stocks', 'amount', 'open', 'high', 'low',
              'close', 'earn', 'volume']

df['date'] = df['date'].str.replace('/', '-')
df['date'] = df['date'].str.replace('106', '2017')
df['date'] = df['date'].str.replace('107', '2018')
df['date'] = df['date'].str.replace('108', '2019')

df['stocks'] = df['stocks'].str.replace(',', '')
df['amount'] = df['amount'].str.replace(',', '')
df['volume'] = df['volume'].str.replace(',', '')
df['earn'] = df['earn'].str.replace('X', '') # some wrong typing in a row?

df['stocks'] = pd.to_numeric(df['stocks'])
df['amount'] = pd.to_numeric(df['amount'])
df['volume'] = pd.to_numeric(df['volume'])
df['earn'] = pd.to_numeric(df['earn'])

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
engine.execute("CREATE TABLE df20190805 (date VARCHAR(255) NOT NULL,\
                                         stocks INT NULL,\
                                         amount INT NULL,\
                                         open INT NULL,\
                                         high INT NULL,\
                                         low INT NULL,\
                                         close INT NULL,\
                                         earn INT NULL,\
                                         volume INT NULL,\
                                         PRIMARY KEY(date))")

# load data into the table
df.to_sql(name='df20190805', con = con, if_exists='replace')
con.close()
print('finished')