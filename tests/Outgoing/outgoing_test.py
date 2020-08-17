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
	rpf = ReorgPageFinder.ReorgPageFinder(logname='./discover1.log', trace=True)
	all_urls = json.load(open('Broken_urls_sample.json', 'r'))['sp']
	sites = sorted(list(all_urls.keys()))
	pieces = 2
	sites = [sites[int(i*len(sites) / pieces):int((i+1)*len(sites) / pieces)] for i in range(pieces)]
	sites = sites[0]
	# sites = ['wikileaks.org']
	
	for i, site in enumerate(sites):
		print(f'SiTENO.{i}: {site}')
		urls = all_urls[site]
		# urls = []
		rpf.init_site(site, urls)
		rpf.discover()

def backpath():
	global sites
	earliest_json = json.load(open('../BackPath/earliest_result.json', 'r'))
	latest_json = json.load(open('../BackPath/latest_result.json', 'r'))
	earliest, latest = {}, {}
	for obj in earliest_json:
		if obj['path']: earliest[obj['url']] = obj
	for obj in latest_json:
		if obj['path']: latest[obj['url']] = obj
	all_urls = json.load(open('Broken_urls_sample.json', 'r'))
	memo = tools.Memoizer()
	similar = tools.Similar()
	bf = discoverer.Backpath_Finder(similar=similar, memo=memo)
	c = 0
	for urls in all_urls['no_sp'].values():
		for url in urls:
			c += 1
			print(f'URLNO{c}: {url}')
			if url in earliest:
				p = earliest[url]
				path = discoverer.Path(url)
				path.path = p['path']
				path.sigs = p['sigs']
				reorg_url = bf.match_path(path)
				if reorg_url:
					reorg_url = reorg_url[0]
					print(f'Found: {reorg_url}')
					db.reorg.update_one({'url': url}, {'$set': {
						'reorg_url_discover_test': reorg_url,
						'by_discover_test': {
							"method": "backpath",
							"suffice": True,
							"type": "earliest"
						}	
					}})
			if url in latest:
				p = latest[url]
				path = discoverer.Path(url)
				path.path = p['path']
				path.sigs = p['sigs']
				reorg_url = bf.match_path(path)
				if reorg_url:
					reorg_url = reorg_url[0]
					print(f'Found: {reorg_url}')
					db.reorg.update_one({'url': url}, {'$set': {
						'reorg_url_discover_test': reorg_url,
						'by_discover_test': {
							"method": "backpath",
							"suffice": True,
							"type": "latest"
						}	
					}})


discover()