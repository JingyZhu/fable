"""
Check whether each page has some date on the page
"""
import os
import json
import sys
from bs4 import BeautifulSoup

sys.path.append('../')
from utils.crawl import chrome_crawl
from utils.text_utils import extract_date

data = json.load(open('status_200.json', 'r'))
data = ["http://web.archive.org/web/20040805145902/http://bigpicture.typepad.com/comments/2004/07/micorosft_chasi.html"]
url_date = {}
for i, url in enumerate(data):
    print(i, url)
    url_date[url] = ""
    html, loc = chrome_crawl(url, screenshot=True)
    if html == "":
        continue
    date = extract_date(html)
    date = date.strftime("%Y %m %d") if date is not None else ""
    if date == "":
        date = extract_date(html, version='mine')
    url_date[url] = [date, loc]
    with open('temp.html', 'w+') as f:
        f.write(html)
        

json.dump(url_date, open('url_date.json', 'w+'))