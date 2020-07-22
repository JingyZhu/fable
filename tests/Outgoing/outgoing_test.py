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

sites = sorted(all_urls.keys(), reverse=True)
# sites = [
# 	# "tradepub.com", # Not good
#     "turismo.gal",
#     "utoronto.ca", # no snapshots
#     "scoop.co.nz",
# ]

# sites = [
#     "araport.org", # Not good
#     "ptb.de", # no snapshot
#     "wlu.edu",
#     "acer.com",
# ]

# sites = [
#     "zhuzhouwang.com",
# ]

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
	rpf = ReorgPageFinder.ReorgPageFinder(logname='./discover1.log')
	# sites = sorted(list(all_urls['discover'].keys()))
	sites = sites[:int(len(sites)/2)]
	# sites = ['wikileaks.org']
	for i, site in enumerate(sites):
		print(f'SiTENO.{i}: {site}')
		urls = all_urls[site]
		# urls = []
		rpf.init_site(site, urls)
		rpf.discover()

discover()