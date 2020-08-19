import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import sys
sys.path.append('../../')
from ReorgPageFinder import StrawmanFinder
import config
from utils import text_utils, url_utils


all_urls = json.load(open('Broken_urls.json', 'r'))
urls_year = json.load(open('Broken_urls_years.json', 'r'))
all_urls = {site: [(url, urls_year[url]) for url in site_urls] for site, site_urls in all_urls.items()}

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder

# sites = sorted(all_urls.keys())
sites = sorted(all_urls.keys())

def search():
	global sites
	rpf = StrawmanFinder.StrawmanFinder(logname='./strawman.log')

	for i, site in enumerate(sites):
		print(f'SiTENO.{i}: {site}')
		urls = all_urls[site]
		rpf.init_site(site, urls)
		rpf.search()

search()