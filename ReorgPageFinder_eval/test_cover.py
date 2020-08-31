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


all_urls = json.load(open('../tests/Outgoing/Broken_urls.json', 'r'))
db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder

# sites = sorted(all_urls.keys())
sites = sorted(all_urls.keys())

def search():
	global sites
	rpfe = ReorgPageFinder_coverage.ReorgPageFinder(logname='./search_cover.log')
	sites = sites[:int(len(sites)/2)]
	for i, site in enumerate(sites):
		print(f'SiTENO.{i}: {site}')
		urls = all_urls[site]
		rpfe.search_by_queries(site, urls)

def discover():
	global sites
	rpf = ReorgPageFinder_coverage.ReorgPageFinder(logname='./discover1.log')
	all_urls = json.load(open('Broken_urls.json', 'r'))
	sites = sorted(list(all_urls.keys()))
	pieces = 2
	sites = [sites[int(i*len(sites) / pieces):int((i+1)*len(sites) / pieces)] for i in range(pieces)]
	sites = sites[0]
	# sites = ['wikileaks.org']
	
	for i, site in enumerate(sites):
		print(f'SiTENO.{i}: {site}')
		urls = all_urls[site]

search()
