"""
Check for false positive
"""
import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os, random
from collections import defaultdict
import time

import sys
sys.path.append('../../')
from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder
import config
from utils import text_utils, url_utils, sic_transit

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder

all_urls = json.load(open('Broken_urls.json', 'r'))
symptom = json.load(open('Broken_urls_symptoms.json', 'r'))

sites = sorted(all_urls.keys())
count = 0

s_map = {}
# Contruct reverse map
for s, d in symptom.items():
    for urls in d.values():
        for url in urls:
            s_map[url] = s

rpf = ReorgPageFinder.ReorgPageFinder(logname='./fp.log')

fps = {'search': [], 'dis': []}

for site in sites:
    for url in all_urls[site]:
        if s_map[url] != 'Soft-404':
            continue
        count += 1
        print(count, url)
        reorg = db.reorg.find_one({'url': url})
        if 'reorg_url_search' in reorg:
            reorg_url = reorg['reorg_url_search']
            check = rpf.fp_check(url, reorg_url)
            if check:
                print('search false positive: ', reorg_url)
                db.na_urls.update_one({'_id': url}, {'$set': {
                    "url": url,
                    "hostname": site,
                    'false_positive_search': True
                }}, upsert=True)
                fps['search'].append(url)
        if 'reorg_url_discover' in reorg:
            reorg_url = reorg['reorg_url_discover']
            check = rpf.fp_check(url, reorg_url)
            if check:
                print('discover false positive: ', reorg_url)
                db.na_urls.update_one({'_id': url}, {'$set': {
                    "url": url,
                    "hostname": site,
                    'false_positive_discover': True
                }}, upsert=True)
                fps['dis'].append(url)

json.dump(fps, open('false_pos.json', 'w+'))        