import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time, random
import pandas as pd

import sys
sys.path.append('../../')
from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder
import config
from utils import url_utils, sic_transit

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder
filter_list = ['.xml', '.pdf', '.jpg', '.jpeg', '.gif', '.png', '.doc', '.ppt']


df = pd.read_csv('soft-404.csv')
urls = json.loads(df.to_json(orient='records'))
urls.sort(key=lambda x: x['url'])

pieces = 4
urls = [urls[int(i*len(urls) / pieces):int((i+1)*len(urls) / pieces)] for i in range(pieces)]
urls = urls[3]
random.shuffle(urls)

for i, url_obj in enumerate(urls):
    url = url_obj['url']
    print(i, url)
    _, ext = os.path.splitext(urlsplit(url).path)
    if ext.lower() in filter_list:
        continue
    is_broken, reasons = sic_transit.broken(url, html=True)
    try:
        db.soft_404.insert_one({
            '_id': url,
            'url': url,
            'year': url_obj['year'],
            'broken': is_broken,
            'reason': reasons
        })
    except Exception as e:
        print(str(e))