'''
web scraping reference:
https://medium.com/renee0918/python-%E7%88%AC%E5%8F%96%E5%80%8B%E8%82%A1%E6%AD%B7%E5%B9%B4%E8%82%A1%E5%83%B9%E8%B3%87%E8%A8%8A-b6bc594c8a95
'''
# Import package
from datetime import date,timedelta
from urllib.request import urlopen
from dateutil import rrule
import matplotlib.pyplot as plt
import datetime
import pandas as pd
import numpy as np
import json
import time

# Define a function to get url and extract the content.
def craw_one_month(stock_number,date):
    # Get url
    # https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&date=20190712&stockNo=2330
    url = (
        "http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date="+
        date.strftime('%Y%m%d')+
        "&stockNo="+
        str(stock_number)
    )
    data = json.loads(urlopen(url).read())
    # get 'data' as values of the dataframe, 'fields' as columns of the dataframe
    return pd.DataFrame(data['data'],columns=data['fields'])

# Define a function to get multiple dataframes and concatenate them together
def craw_stock(stock_number, start_month):
    # define beginning time
    # without date() will give you a list(string)
    b_month = date(*[int(x) for x in start_month.split('-')])
    # Get only "Y-m-d". Remove strftime will give you "Y-m-d h:m:s.xxxxx".
    now = datetime.datetime.now().strftime("%Y-%m-%d")        
    # define ending time
    e_month = date(*[int(x) for x in now.split('-')])
    
    # create an empty dataframe
    result = pd.DataFrame()
    # create multiple date from b_month to e_month
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=b_month, until=e_month):
        # concat empty dataframe with crawed one and ignore the index
        result = pd.concat([result,craw_one_month(stock_number,dt)],ignore_index=True)
        # Stop for 5 sec after picking up one set of data
        time.sleep(5000.0/1000.0);
    '''
    minimum sleep time is not 2 sec, it seems the larger the data, the longer the 
    sleep time.
    '''
    return result

# how to scrap more than one stock? Should modify the function?
df = craw_stock(2317,"2017-01-01")
# data cleaning
df.columns = ['date', 'stocks', 'amount', 'open', 'high', 'low',
              'close', 'earn', 'volume']

df['date'] = df['date'].str.replace('/', '-')
df['date'] = df['date'].str.replace('106', '2017')
df['date'] = df['date'].str.replace('107', '2018')
df['date'] = df['date'].str.replace('108', '2019')

df['stocks'] = df['stocks'].str.replace(',', '')
df['amount'] = df['amount'].str.replace(',', '')
df['volume'] = df['volume'].str.replace(',', '')
df['earn'] = df['earn'].str.replace('X', '') # a weird typo in the original data

df['stocks'] = pd.to_numeric(df['stocks'])
df['amount'] = pd.to_numeric(df['amount'])
df['volume'] = pd.to_numeric(df['volume'])
df['earn'] = pd.to_numeric(df['earn'])

# Load dataframe to mysql
# Can pymysql do this?
from sqlalchemy import create_engine

engine = create_engine("mysql://root:py9m9s34@localhost/stocks")
con = engine.connect()
# Create table in mysql

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

# load dataframe into the table
df.to_sql(name='df20190805', con = con, if_exists='replace')
con.close()
print('finished')
