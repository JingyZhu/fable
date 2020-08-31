import searcher_coverage, tools
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
        # self.discoverer = discoverer.Discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
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
            search_trace = self.searcher.search(url)
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
    
    def discover(self, infer=False):
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        # _discover
        noreorg_urls = self.db.reorg.find({"hostname": self.site, 'reorg_url_discover_test': {"$exists": False}})
        discovered_checked = self.db.checked.find({"hostname": self.site, "discover": True})
        discovered_checked = set([sc['url'] for sc in discovered_checked])
        urls = [u for u in noreorg_urls if u['url'] not in discovered_checked ]
        broken_urls = set([bu['url'] for bu in urls])
        self.logger.info(f'Discover SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            method, suffice = 'discover', False
            while True: # Dummy while lloop served as goto
                discovered = self.discoverer.wayback_alias(url)
                if discovered:
                    fp = self.fp_check(url, discovered)
                    if fp:
                        discovered = None
                    else:
                        trace = {'suffice': True, 'type': 'wayback_alias', 'value': None}
                        break

                discovered, trace = self.discoverer.bf_find(url, policy='latest')
                if trace.get('backpath'):
                    try:
                        self.db.trace.update_one({'_id': url}, {"$set": {
                            "url": url,
                            "hostname": self.site,
                            "backpath_latest": trace['backpath']
                        }}, upsert=True)
                    except Exception as e:
                        self.logger.warn(f'Discover update trace backpath: {str(e)}')
                if discovered:
                    method = 'backpath_latest'
                    break

                discovered, trace = self.discoverer.discover(url)
                try:
                    self.db.trace.update_one({'_id': url}, {"$set": {
                        "url": url,
                        "hostname": self.site,
                        "discover": trace['trace']
                    }}, upsert=True)
                except Exception as e:
                    self.logger.warn(f'Discover update trace discover: {str(e)}')
                if discovered:
                    break
                suffice = trace['suffice']

                discovered, trace = self.discoverer.bf_find(url, policy='earliest')
                if trace.get('backpath'):
                    try:
                        self.db.trace.update_one({'_id': url}, {"$set": {
                            "url": url,
                            "hostname": self.site,
                            "backpath_earliest": trace['backpath']
                        }}, upsert=True)
                    except Exception as e:
                        self.logger.warn(f'Discover update trace backpath: {str(e)}')
                if discovered:
                    method = 'backpath_earliest'
                    break
                break

            update_dict = {}
            has_title = self.db.reorg.find_one({'url': url})
            # if has_title is None: # No longer in reorg (already deleted)
            #     continue
            if 'title' not in has_title:
                try:
                    wayback_url = self.memo.wayback_index(url)
                    html = self.memo.crawl(wayback_url)
                    title = self.memo.extract_title(html, version='domdistiller')
                except: # No snapthost on wayback
                    self.logger.error(f'WB_Error {url}: Fail to get data from wayback')
                    try: self.db.na_urls.update_one({'_id': url}, {"$set": {
                        'url': url,
                        'hostname': self.site,
                        'no_snapshot': True
                    }}, upsert=True)
                    except: pass
                    title = 'N/A'
                update_dict = {'title': title}
            else:
                title = has_title['title']


            if discovered is not None:
                self.logger.info(f'Found reorg: {discovered}')
                fp = self.fp_check(url, discovered)
                if not fp: # False positive test
                    # _discover
                    update_dict.update({'reorg_url_discover_test': discovered, 'by_discover_test':{
                        "method": method
                    }})
                    by_discover = {k: v for k, v in trace.items() if k not in ['trace', 'backpath', 'suffice']}
                    # discover
                    update_dict['by_discover_test'].update(by_discover)
                else:
                    # discover
                    try: self.db.na_urls.update_one({'_id': url}, {'$set': {
                            'url': url,
                            'false_positive_discover_test': True, 
                            'hostname': self.site
                        }}, upsert=True)
                    except: pass
                    discovered = None
            elif not suffice:
                try:
                    self.db.na_urls.update_one({'_id': url}, {'$set': {
                        'no_working_parent': True, 
                        'hostname': self.site
                    }}, upsert=True)
                except:pass


            if len(update_dict) > 0:
                try:
                    self.db.reorg.update_one({'url': url}, {'$set': update_dict})
                except Exception as e:
                    self.logger.warn(f'Discover update DB: {str(e)}')
            discovered_checked.add(url)
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    "discover": True
                }}, upsert=True)
            except Exception as e:
                self.logger.warn(f'Discover update checked: {str(e)}')
            
            if not infer:
                continue
            # TEMP
            # if discovered is not None:
            #     example = ((url, title), discovered)
            #     added = self._add_url_to_patterns(*unpack_ex(example))
            #     if not added:
            #         continue
            #     success = self.query_inferer([example])
            #     while len(success) > 0:
            #         added = False
            #         for suc in success:
            #             broken_urls.discard(unpack_ex(suc)[0])
            #             a = self._add_url_to_patterns(*unpack_ex(suc))
            #             added = added or a
            #         if not added:
            #             break
            #         examples = success
            #         success = self.query_inferer(examples)
