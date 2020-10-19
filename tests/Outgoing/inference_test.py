import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import sys
sys.path.append('../../')
from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder
import config
from utils import text_utils, url_utils

all_urls = json.load(open('Broken_urls_infer.json', 'r'))
# sites = sorted(all_urls.keys())
sites = sorted(all_urls.keys())

def search_discover():
    global sites
    site = 'nasa.gov'
    rpf = ReorgPageFinder.ReorgPageFinder(logname=f'{site}.log')
    pieces = 4
    sites = [sites[int(i*len(sites) / pieces):int((i+1)*len(sites) / pieces)] for i in range(pieces)]
    sites = sites[3]
    # TMP
    sites = [site]
    for i, site in enumerate(sites):
        print(f'SiTENO.{i}: {site}')
        urls = all_urls[site]
        rpf.init_site(site, urls)
        rpf.first_search()

        rpf.second_search()
        rpf.discover()

def infer():
    global sites
    rpf = ReorgPageFinder.ReorgPageFinder(logname='./infer_tmp.log')
    pieces = 2
    sites = [sites[int(i*len(sites) / pieces):int((i+1)*len(sites) / pieces)] for i in range(pieces)]
    sites = sites[0]

    sites = ['wordpress.com']
    for i, site in enumerate(sites):
        print(f'SiTENO.{i}: {site}')
        urls = all_urls[site]
        # urls = []
        rpf.init_site(site, urls)
        rpf.infer()

# search_discover()
infer()
