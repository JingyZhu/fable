from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time
import json

import config
from utils import text_utils, url_utils

db_wd = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
# sites = json.load(open('reorg_benchmark.json', 'r'))
# sites.sort()
# sites =  [
# 	"bottrop.de",
#     "bowdoin.edu",
#     "bto.org",
# ]

# sites = [
# 	"autocasion.com",
#     "baltimoresun.com",
#     "bargainbriana.com",
#     "blooloop.com",
# ]

sites = [
	"cactus-mall.com",
    "cdharrison.com",
    "cityofmesquite.com"
]

sites = [
	"climbtothestars.org",
    "comptia.org",
    "csu.edu",
    "drive.com.au"
]

rpf = ReorgPageFinder.ReorgPageFinder(logname='./ReorgPageFinder3.log')

for site in sites:
	urls = db_wd.url_status_implicit_broken.find({
		"$and": [
			{'$or': [{'broken': True}, {'sic_broken': True}]},
			{'hostname': site}
	]})
	urls = [url['url'] for url in urls]
	rpf.init_site(site, urls)
	rpf.infer()
	rpf.first_search()
	rpf.second_search()
	rpf.discover()
	rpf.infer()
