import searcher_coverage, discoverer_efficiency, tools
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit, parse_qsl
import os
from collections import defaultdict
import time
import json
import logging
import sys

import config
from utils import text_utils, url_utils, crawl, sic_transit

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder
he = url_utils.HostExtractor()

class ReorgPageFinder:
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, logger=None, logname=None):
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.PS = crawl.ProxySelector(proxies)
        self.searcher = searcher_coverage.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.discoverer = discoverer_efficiency.Discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
        # self.inferer = inferer.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
        self.pattern_dict = None
        self.seen_reorg_pairs = None
        self.logname = './ReorgPageFinder.log' if logname is None else logname
        self.logger = logger if logger is not None else self._init_logger()

    def _init_logger(self):
        logger = logging.getLogger('logger')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s %(asctime)s [%(filename)s %(funcName)s:%(lineno)s]: \n %(message)s')
        file_handler = logging.FileHandler(self.logname)
        file_handler.setFormatter(formatter)
        std_handler = logging.StreamHandler(sys.stdout)
        std_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(std_handler)
        return logger
    
    def init_site(self, site):
        self.site = site

    def search_by_queries(self, site, required_urls):
        required_urls = set(required_urls)
        site_urls = db.reorg.find({"hostname": site})
        searched_checked = db.checked.find({"hostname": self.site, "search_coverage": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        urls = [u for u in site_urls if u['url'] not in searched_checked and u['url'] in required_urls]
        broken_urls = set([(u['url'], u.get('reorg_url_search')) for u in urls])
        self.logger.info(f'Search coverage SITE: {site} #URLS: {len(broken_urls)}')
        i = 0
        self.similar.clear_titles()
        while len(broken_urls) > 0:
            url,reorg_url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            # TODO Change with requirements
            search_trace = self.searcher.search_title_exact_site(url)
            if search_trace is None:
                continue

            search_trace.update({
                'url': url,
                'hostname': he.extract(url)
            })
            if reorg_url:
                search_trace.update({'reorg_url': reorg_url})
            try:
                self.db.search_trace.update_one({'_id': url}, {'$set': search_trace}, upsert=True)
            except Exception as e:
                self.logger.warn(f'Search_cover update search_trace: {str(e)}')
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    "search_coverage": True
                }}, upsert=True)
            except Exception as e:
                self.logger.warn(f'Search_cover update checked: {str(e)}')
    
    def discover(self, site, required_urls, search_type):
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        # _discover
        assert(search_type in {'BFS', 'DFS'})
        required_urls = set(required_urls)
        site_urls = self.db.reorg.find({"hostname": self.site})
        discovered_checked = self.db.checked.find({"hostname": self.site, f"discover_{search_type}": True})
        discovered_checked = set([sc['url'] for sc in discovered_checked])
        urls = [u for u in site_urls if u['url'] not in discovered_checked and u['url'] in required_urls]
        broken_urls = set([(u['url'], u.get('by_discover_test', u.get('by_discover', {'type': 'title'}))['type']) for u in urls])
        self.logger.info(f'Discover SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url, reorg_type = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            method, suffice = 'discover', False
            while True: # Dummy while lloop served as goto

                # discovered, trace = self.discoverer.bf_find(url, policy='latest')
                # if trace.get('backpath'):
                #     try:
                #         self.db.trace.update_one({'_id': url}, {"$set": {
                #             "url": url,
                #             "hostname": self.site,
                #             "backpath_latest": trace['backpath']
                #         }}, upsert=True)
                #     except Exception as e:
                #         self.logger.warn(f'Discover update trace backpath: {str(e)}')
                # if discovered:
                #     method = 'backpath_latest'
                #     break

                discovered, trace = self.discoverer.discover(url, search_type=search_type, reorg_type=reorg_type)
                try:
                    self.db.trace.update_one({'_id': url}, {"$set": {
                        "url": url,
                        "hostname": self.site,
                        f"discover_{search_type}": trace['trace']
                    }}, upsert=True)
                except Exception as e:
                    self.logger.warn(f'Discover update trace discover: {str(e)}')
                if discovered:
                    break
                suffice = trace['suffice']

                # discovered, trace = self.discoverer.bf_find(url, policy='earliest')
                # if trace.get('backpath'):
                #     try:
                #         self.db.trace.update_one({'_id': url}, {"$set": {
                #             "url": url,
                #             "hostname": self.site,
                #             "backpath_earliest": trace['backpath']
                #         }}, upsert=True)
                #     except Exception as e:
                #         self.logger.warn(f'Discover update trace backpath: {str(e)}')
                # if discovered:
                #     method = 'backpath_earliest'
                #     break
                break

            # update_dict = {}
            # has_title = self.db.reorg.find_one({'url': url})

            # if 'title' not in has_title:
            #     try:
            #         wayback_url = self.memo.wayback_index(url)
            #         html = self.memo.crawl(wayback_url)
            #         title = self.memo.extract_title(html, version='domdistiller')
            #     except: # No snapthost on wayback
            #         self.logger.error(f'WB_Error {url}: Fail to get data from wayback')
            #         try: self.db.na_urls.update_one({'_id': url}, {"$set": {
            #             'url': url,
            #             'hostname': self.site,
            #             'no_snapshot': True
            #         }}, upsert=True)
            #         except: pass
            #         title = 'N/A'
            #     update_dict = {'title': title}
            # else:
            #     title = has_title['title']


            discovered_checked.add(url)
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    f"discover_{search_type}": True
                }}, upsert=True)
            except Exception as e:
                self.logger.warn(f'Discover update checked: {str(e)}')
            
