from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("mysql://root:py9m9s34@localhost/stocks")

con = engine.connect()

df = pd.read_csv('/home/magic/df.csv', index_col=0)

df.to_sql(name='df20190707', con = con, if_exists='replace')
con.close()

'''
Need to create mysql table first
'''