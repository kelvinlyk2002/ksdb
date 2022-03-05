import urllib.request, urllib.parse, urllib.error
import sqlite3
import statistics
import ssl
import re
from collections import Counter
from bs4 import BeautifulSoup
from datetime import datetime
import time

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Creating Table
conn = sqlite3.connect('url.sqlite')
cur = conn.cursor()
cur.executescript('''
CREATE TABLE IF NOT EXISTS UrlDB (
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    projectURL    TEXT);
''')

# Only 200 Pages available to be crawled
startPage = int(1)
endPage = int(200)

for page in range(startPage, endPage + 1):
    url = "https://www.kickstarter.com/discover/advanced?state=successful&sort=most_funded"+"&page="+str(page)
    print("Start Retrieving", url)

    main_html = urllib.request.urlopen(url, context=ctx).read()
    main_data = main_html.decode()
    raw_soup = str(BeautifulSoup(main_data, 'html.parser'))
    rawProjURLList = re.findall("https://www.kickstarter.com/projects/.+/.+\"",raw_soup.replace("&quot;","\""))

    projURLList = []
    for rawProjURL in rawProjURLList:
        projURL = rawProjURL.split("\"")[0]
        if projURL == "https://www.kickstarter.com/projects/feed.atom":
            continue
        else:
            projURLList.append(projURL)
            cur.execute('''INSERT INTO UrlDB (projectURL) VALUES (:url)''', {"url": projURL} )
        
    conn.commit()
    time.sleep(2)
