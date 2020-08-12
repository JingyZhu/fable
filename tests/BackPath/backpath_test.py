import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os, time
from collections import defaultdict

import sys
sys.path.append('../../')
from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder
import config
from utils import text_utils, url_utils

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder

all_urls = json.load(open('test_urls.json', 'r'))

memo = tools.Memoizer()
similar = tools.Similar()
policy = 'latest'
bf = discoverer.Backpath_Finder(policy=policy, memo=memo, similar=similar)
results = json.load(open(f'{policy}_result.json', 'r'))
got = {r['url'] for r in results}
for i, url in enumerate(all_urls):
    print(f"URLNO {i}: ", url)
    if url in got: continue
    if urlsplit(url).path in {'', '/'}: continue
    path = bf.find_path(url)
    results.append({
        'url': url,
        'path': path.path if path else None,
        'sigs': path.sigs if path else None
    })
    json.dump(results, open(f'{policy}_result.json', 'w+'), indent=2)