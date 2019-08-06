from datetime import date, timedelta
from urllib.request import urlopen
import datetime
import pandas as pd
import csv

url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&date=20190712&stockNo=2330"
c = pd.read_csv(url, encoding="ISO-8859-1")
# can encoding but wrong words
b = c.dropna(axis='columns')
b
