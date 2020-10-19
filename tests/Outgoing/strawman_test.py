import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import sys
sys.path.append('../../')
from ReorgPageFinder import StrawmanFinder_adv, StrawmanFinder
import config
from utils import text_utils, url_utils


all_urls = json.load(open('Broken_urls.json', 'r'))
urls_year = json.load(open('Broken_urls_years.json', 'r'))
# --->adv
# all_urls = {site: [(url, urls_year[url]) for url in site_urls] for site, site_urls in all_urls.items()}

# sites = sorted(all_urls.keys())
sites = sorted(all_urls.keys())

def search():
	global sites
	# --> adv
	rpf = StrawmanFinder_adv.StrawmanFinder(logname='./strawman_adv.log')
	pieces = 4
	sites = [sites[int(i*len(sites) / pieces):int((i+1)*len(sites) / pieces)] for i in range(pieces)]
	sites = sites[2]
	for i, site in enumerate(sites):
		print(f'SiTENO.{i}: {site}')
		urls = all_urls[site]
		rpf.init_site(site, urls)

		# --> adv
		# urls = [u[0] for u in urls]
		rpf.search(required_urls=urls)

search()