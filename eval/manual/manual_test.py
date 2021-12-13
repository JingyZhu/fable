import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import sys
sys.path.append('../../')
from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder_deploy
import config
from utils import text_utils, url_utils


all_urls = json.load(open('Broken_urls_manual.json', 'r'))

sites = sorted(all_urls.keys())

black_list = {'ebates.com', 'eclipse.org'}
rpfd = ReorgPageFinder_deploy.ReorgPageFinder(logname='./ReorgPageFinder_2.log')

pieces = 4
sites = [sites[int(i*len(sites) / pieces):int((i+1)*len(sites) / pieces)] for i in range(pieces)]
sites = sites[1]

for i, site in enumerate(sites):
    if site in black_list:
        continue
    print(f'SiTENO.{i}: {site}')
    urls = all_urls[site]
    rpfd.init_site(site, urls)
    rpfd.second_search(infer=True, required_urls=urls)
    rpfd.discover(infer=True, required_urls=urls)
    rpfd.infer()
