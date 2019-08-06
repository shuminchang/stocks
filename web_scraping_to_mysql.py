'''
CREATE TABLE Scrap_pages (id BIGINT(7) NOT NULL AUTO_INCREMENT,
title VARCHAR(200), content VARCHAR(10000),PRIMARY KEY(id));
'''

'''
ALTER DATABASE scrap CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
ALTER TABLE Scrap_pages CONVERT TO CHARACTER SET utf8mb4 COLLATE
utf8mb4_unicode_ci;
ALTER TABLE Scrap_pages CHANGE title title VARCHAR(200) CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
ALTER TABLE pages CHANGE content content VARCHAR(10000) CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
'''

from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime
import random
import pymysql
import re

# make a connection
conn = pymysql.connect(host='127.0.0.1',user='root', passwd = None, db = 'mysql',
charset = 'utf8')
cur = conn.cursor()
cur.execute("USE scrap")
random.seed(datetime.datetime.now())
def store(title, content):
   cur.execute('INSERT INTO scrap_pages (title, content) VALUES ''("%s","%s")', (title, content))
   cur.connection.commit()
   
# connect with wikipedia and get data from it
def getLinks(articleUrl):
   html = urlopen('http://en.wikipedia.org'+articleUrl)
   bs = BeautifulSoup(html, 'html.parser')
   title = bs.find('h1').get_text()
   content = bs.find('div', {'id':'mw-content-text'}).find('p').get_text()
   store(title, content)
   return bs.find('div', {'id':'bodyContent'}).findAll('a',href=re.compile('^(/wiki/)((?!:).)*$'))
links = getLinks('/wiki/Kevin_Bacon')
try:
   while len(links) > 0:
      newArticle = links[random.randint(0, len(links)-1)].attrs['href']
      print(newArticle)
      links = getLinks(newArticle)

# close both cursor and connection
finally:
   cur.close()
   conn.close()
   
# will continuously running, how to stop it?