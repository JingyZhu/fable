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


all_urls = json.load(open('Broken_urls.json', 'r'))

# sites = [
# 	"uwindsor.ca",
#     "sina.com.cn",
#     "wprost.pl",
#     "culture.fr",
# ]

# sites = [
#     "xhby.net",
# ]

sites = [
    "zhuzhouwang.com",
]

rpf = ReorgPageFinder.ReorgPageFinder(logname='./Outgoing3.log')

for site in sites:
	urls = all_urls[site]
	rpf.init_site(site, urls)
	rpf.infer()
	rpf.first_search()
	rpf.second_search()
	rpf.discover()
	rpf.infer()