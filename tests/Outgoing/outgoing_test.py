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
db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder

# sites = sorted(all_urls.keys())
sites = sorted(all_urls.keys(), reverse=True)


# rpf = ReorgPageFinder.ReorgPageFinder(logname='./Outgoing2.log')

def search():
	global sites
	rpf = ReorgPageFinder.ReorgPageFinder(logname='./search2.log')
	sites = sites[int(len(sites)/2):]
	for i, site in enumerate(sites):
		print(f'SiTENO.{i}: {site}')
		urls = all_urls[site]
		rpf.init_site(site, urls)
		rpf.first_search()
		rpf.second_search()

def discover():
	global sites
	rpf = ReorgPageFinder.ReorgPageFinder(logname='./discover6.log', trace=True)
	# sites = sorted(list(all_urls['discover'].keys()))
	pieces = 4
	sites = [sites[int(i*len(sites) / pieces):int((i+1)*len(sites) / pieces)] for i in range(pieces)]
	sites = sites[3]
	# sites = ['wikileaks.org']
	for i, site in enumerate(sites):
		print(f'SiTENO.{i}: {site}')
		urls = all_urls[site]
		# urls = []
		rpf.init_site(site, urls)
		rpf.discover()

discover()