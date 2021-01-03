import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit, parse_qsl
import os
from collections import defaultdict
import time
import json
import logging
import sys

from . import discoverer_efficiency
from fable import config, tracer, tools
from fable.tracer import tracer as tracing
from fable.utils import text_utils, url_utils, crawl, sic_transit

db = config.DB
he = url_utils.HostExtractor()


class ReorgPageFinder:
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, tracer=None,\
                classname='backlink_eff', logname=None, loglevel=logging.INFO):
        """ 
        memo: tools.Memoizer class for access cached crawls & API calls. If None, initialize one.
        similar: tools.Similar class for similarity matching. If None, initialize one.
        tracer: self-extended logger
        classname: Class (key) that the db will update data in the corresponding document
        logname: The log file name that will be output msg. If not specified, use classname
        """
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.PS = crawl.ProxySelector(proxies)
        # self.searcher = searcher.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.discoverer = discoverer_efficiency.Discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
        # self.inferer = inferer.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
        # self.pattern_dict = None
        # self.seen_reorg_pairs = None
        self.classname = classname
        self.logname = classname if logname is None else logname
        self.tracer = tracer if tracer is not None else self._init_tracer(loglevel=loglevel)
        self.inference_classes = [self.classname] # * Classes looking on reorg for aliases sets

    def _init_tracer(self, loglevel):
        logging.setLoggerClass(tracing)
        tracer = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tracer._set_meta(self.classname, logname=self.logname, db=self.db, loglevel=loglevel)
        return tracer
    
    def set_inferencer_classes(self, classes):
        self.inference_classes = classes + [self.classname]

    def init_site(self, site, urls):
        self.site = site
        objs = []
        already_in = list(self.db.reorg.find({'hostname': site}))
        already_in = set([u['url'] for u in already_in])
        for url in urls:
            if url in already_in:
                continue
            objs.append({'url': url, 'hostname': site})
            # TODO May need avoid insert false positive 
            if not sic_transit.broken(url):
                try:
                    self.db.na_urls.update_one({'_id': url}, {'$set': 
                        {'url': url, 'hostname': site, 'url': url, 'false_positive': True}}, upsert=True)
                except: pass
        try:
            self.db.reorg.insert_many(objs, ordered=False)
        except: pass
        
        if len(self.tracer.handlers) > 2:
            self.tracer.handlers.pop()
        formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
        if not os.path.exists('logs'):
            os.mkdir('logs')
        file_handler = logging.FileHandler(f'./logs/{site}.log')
        file_handler.setFormatter(formatter)
        self.tracer.addHandler(file_handler)

    def clear_site(self):
        self.site = None
        self.pattern_dict = None
        self.seen_reorg_pairs = None
        self.tracer.handlers.pop()

    def fp_check(self, url, reorg_url):
        """
        Determine False Positive

        returns: Boolean on if false positive
        """
        if url_utils.url_match(url, reorg_url):
            return True
        html, url = self.memo.crawl(url, final_url=True)
        reorg_html, reorg_url = self.memo.crawl(reorg_url, final_url=True)
        if html is None or reorg_html is None:
            return False
        content = self.memo.extract_content(html)
        reorg_content = self.memo.extract_content(reorg_html)
        self.similar.tfidf._clear_workingset()
        simi = self.similar.tfidf.similar(content, reorg_content)
        return simi >= 0.8
    

    def discover(self, search_type, required_urls=None):
        """
        infer: Infer every time when a new alias is found
        Required urls: URLs that will be run on
        """
        if self.similar.site is None or self.site not in self.similar.site:
            self.similar.clear_titles()
            if not self.similar._init_titles(self.site):
                self.tracer.warn(f"Similar._init_titles: Fail to get homepage of {self.site}")
                return
        # ! discover
        assert(search_type in {'BFS', 'DFS'})
        noreorg_urls = list(self.db.reorg.find({"hostname": self.site, self.classname: {"$exists": False}}))
        discovered_checked = self.db.checked.find({"hostname": self.site, f"{self.classname}.discover": True})
        discovered_checked = set([sc['url'] for sc in discovered_checked])
        
        required_urls = set(required_urls) if required_urls else set([u['url'] for u in noreorg_urls])
        
        urls = [u for u in noreorg_urls if u['url'] not in discovered_checked and u['url'] in required_urls]
        broken_urls = set([bu['url'] for bu in urls])
        self.tracer.info(f'Discover SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.tracer.info(f'URL: {i} {url}')
            method, suffice = 'discover', False
            while True: # Dummy while lloop served as goto
                # self.tracer.info("Start wayback alias")
                # discovered = self.discoverer.wayback_alias(url)
                # if discovered:
                #     fp = self.fp_check(url, discovered)
                #     if fp:
                #         discovered = None
                #     else:
                #         trace = {'suffice': True, 'type': 'wayback_alias', 'value': None}
                #         break
                
                # self.tracer.info("Start backpath (latest)")
                # discovered, trace = self.discoverer.bf_find(url, policy='latest')
                # if discovered:
                #     method = 'backpath_latest'
                #     break
                
                self.tracer.info("Start discover")
                discovered, trace = self.discoverer.discover(url, search_type=search_type)
                if discovered:
                    break
                suffice = trace['suffice']

                break

            self.tracer.flush()
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
                    self.tracer.error(f'WB_Error {url}: Fail to get data from wayback')
                    try: self.db.na_urls.update_one({'_id': url}, {"$set": {
                        'url': url,
                        'hostname': self.site,
                        'no_snapshot': True
                    }}, upsert=True)
                    except: pass
                    title = 'N/A'
            else:
                title = has_title['title']


            if discovered is not None:
                self.tracer.info(f'Found reorg: {discovered}')
                fp = self.fp_check(url, discovered)
                if not fp: # False positive test
                    # ! discover
                    update_dict.update({'reorg_url': discovered, 'by':{
                        "method": method
                    }})
                    by_discover = {k: v for k, v in trace.items() if k not in ['trace', 'backpath', 'suffice']}
                    # ! discover
                    update_dict['by'].update(by_discover)
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
                    self.db.reorg.update_one({'url': url}, {'$set': {self.classname: update_dict, 'title': title}})
                except Exception as e:
                    self.tracer.warn(f'Discover update DB: {str(e)}')
            discovered_checked.add(url)
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    f"{self.classname}.discover": True
                }}, upsert=True)
            except Exception as e:
                self.tracer.warn(f'Discover update checked: {str(e)}')