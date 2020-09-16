import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import sys
sys.path.append('../../')
import ReorgPageFinder_coverage
import config
from utils import text_utils, url_utils


all_urls = json.load(open('Broken_outlinks_matter.json', 'r'))
db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder

sites = sorted(all_urls.keys())

rpfc = ReorgPageFinder_coverage.ReorgPageFinder(logname='./ReorgPageFinder_1.log')

pieces = 2
sites = [sites[int(i*len(sites) / pieces):int((i+1)*len(sites) / pieces)] for i in range(pieces)]
sites = sites[0]

for i, site in enumerate(sites):
    print(f'SiTENO.{i}: {site}')
    urls = all_urls[site]
    rpfc.search_outlinks(site, urls)