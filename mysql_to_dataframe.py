# Load mysql table to dataframe
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


# sqlalchemy engine
engine = create_engine(URL(
    drivername="mysql",
    username="root",
    password="py9m9s34",
    host="localhost",
    database="stocks"
))

conn = engine.connect()

query = 'SELECT * FROM tsmc'
df = pd.read_sql(sql=query,  # mysql query
                           con=conn)  # size you want to fetch each time

df