"""
Check whether each page has some date on the page
"""
from dateutil import parser as dparser
import os
import json
import sys
from bs4 import BeautifulSoup

sys.path.append('../')
from utils.crawl import chrome_crawl

data = json.load(open('status_200.json', 'r'))
data = ["http://web.archive.org/web/20140325144851/http://searchengineland.com/consumer-watchdog-claims-google-shopping-makes-consumers-pay-higher-prices-more-evidence-of-googles-search-monopoly-178940"]
url_date = {}
for i, url in enumerate(data):
    print(i, url)
    url_date[url] = ""
    html = chrome_crawl(url)
    f = open('temp.html', 'w+')
    f.write(html)
    f.close()
    if html == "":
        continue
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ')
    try:
        dt, fuzzy_token = dparser.parse(text, fuzzy_with_tokens=True)
    except:
        continue
    url_date[url] = dt.strftime("%Y %m %d")
    print(url_date[url], fuzzy_token[0].split()[-1], fuzzy_token[-1].split()[0])

json.dump(url_date, open('url_date.json', 'w+'))