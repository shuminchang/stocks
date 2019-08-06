import pymysql
import pandas as pd
import sys

def mysql_to_csv(sql, file_path, host, user, password):
    try:
        con = pymysql.connect(host=host,
                              user=user,
                              password=password)
        print('Connected to DB: {}'.format(host))
        df = pd.read_sql(sql, con)
        df.to_csv(file_path, encoding='utf-8', header = True,
                  doublequote = True, sep=',', index = False)
        print('File, {}, has been created successfully'.format(file_path))
        con.close()
        
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)
        
sql = 'SELECT * FROM events.potluck'
file_path = '/home/magic/potluck.csv'
host = 'localhost'
user = 'root'
password = 'py9m9s34'
mysql_to_csv(sql, file_path, host, user, password)