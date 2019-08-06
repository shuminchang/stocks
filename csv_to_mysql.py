import pymysql
import pandas as pd
import sys

def csv_to_mysql(load_sql, host, user, password):
    try:
        con = pymysql.connect(host=host,
                              user=user,
                              password=password,
                              autocommit=True,
                              local_infile=1)
        print('Connected to DB: {}'.format(host))
        cursor = con.cursor()
        cursor.execute(load_sql)
        print('Successfully loaded the table from csv.')
        con.close()
    
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)
        
# Need to create mysql table first.
load_sql = "LOAD DATA LOCAL INFILE '/home/magic/df.csv' \
            INTO TABLE stocks.test \
            FIELDS TERMINATED BY ',';"
host = 'localhost'
user = 'root'
password = 'py9m9s34'
csv_to_mysql(load_sql, host, user, password)