from fable import discoverer, searcher, inferer, tools
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit, parse_qsl
import os
from collections import defaultdict
import time
import json
import logging

from . import config
from .tracer import tracer as tracing
from .utils import url_utils, crawl, sic_transit

db = config.DB
he = url_utils.HostExtractor()


class ReorgPageFinder:
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, tracer=None,\
                classname='fable', logname=None, loglevel=logging.INFO):
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
        self.searcher = searcher.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.discoverer = discoverer.Discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
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

    def init_site(self, site, urls=[]):
        self.site = site
        # * Add new URLs to reorg first 
        already_in = list(self.db.reorg.find({'hostname': site}))
        already_in_set = set([u['url'] for u in already_in])
        objs = []
        for url in urls:
            if url in already_in_set:
                continue
            objs.append({'url': url, 'hostname': site})
        try:
            self.db.reorg.insert_many(objs, ordered=False)
        except: pass
        # * Initialize Inferer
        self.inferer.init_site(site)
        site_reorg_urls = self.db.reorg.find({'hostname': site})
        # ? Whether to infer on all classes, all only one? 
        # ? reorg_urls = [reorg for reorg in reorg_urls if len(set(reorg.keys()).intersection(reorg_keys)) > 0]
        for reorg_url in list(site_reorg_urls):
            # reorg_tech = []
            for iclass in self.inference_classes:
                if len(reorg_url.get(iclass, {})) > 0:
                    self.inferer.add_url_alias(reorg_url['url'], (reorg_url.get('title', ''),), reorg_url[iclass]['reorg_url'])
                else:
                    self.inferer.add_url(reorg_url['url'], (reorg_url.get('title', ''),))
        # * Initialized tracer
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

    def _fp_check(self, url, reorg_url):
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

    def infer_new(self, url, meta, alias):
        """
        Called whenever search/discover found new aliases
        Return: [URLs found aliases through inference]
        """
        new_finds = []
        self.inferer.add_url_alias(url, meta, alias)
        example = (url, meta, alias)
        found_aliases = self.inferer.infer_new(example)
        for infer_url, (infer_alias, reason) in found_aliases.items():
            title = self.db.reorg.find_one({'url': infer_url}).get('title', '')
            update_dict = {"reorg_url": infer_alias, "by": {"method": "infer"}}
            update_dict['by'].update(reason)
            self.db.reorg.update_one({'url': infer_url}, {'$set': {self.classname:update_dict}})
            added = self.inferer.add_url_alias(infer_url, (title,), infer_alias)
            any_added = any_added or added
            new_finds.append(infer_url)
        # if not any_added: 
        #     break
        # examples = success
        # found_aliases = self.inferer.infer_new(examples)
        return new_finds


    def infer(self):
        """TODO: What needs to be logged?"""
        found_aliases = self.inferer.infer_all()
        for infer_url, (infer_alias, reason) in found_aliases.items():
            title = self.db.reorg.find_one({'url': infer_url}).get('title', '')
            update_dict = {"reorg_url": infer_alias, "by": {"method": "infer"}}
            update_dict['by'].update(reason)
            self.db.reorg.update_one({'url': infer_url}, {'$set': {self.classname:update_dict}})
            added = self.inferer.add_url_alias(infer_url, (title,), infer_alias)
            any_added = any_added or added
        self.tracer.flush()


    def search(self, required_urls, infer=False, title=True):
        """
        infer: Infer every time when a new alias is found
        Required urls: URLs that will be run on (no checked)
        title: Whether title comparison is taken into consideration
        """
        if not title:
            self.similar.clear_titles()
        elif self.similar.site is None or self.site not in self.similar.site:
            self.similar.clear_titles()
            if not self.similar._init_titles(self.site):
                self.tracer.warn(f"Similar._init_titles: Fail to get homepage of {self.site}")
                return
        # !_search
        reorg_checked = list(self.db.reorg.find({"hostname": self.site, self.classname: {"$exists": True}}))
        reorg_checked = set([u['url'] for u in reorg_checked])
        broken_urls = set([ru for ru in required_urls if ru not in reorg_checked])

        self.tracer.info(f'Search SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.tracer.info(f'URL: {i} {url}')
            start = time.time()
            searched = None
            searched = self.searcher.search(url, search_engine='bing')
            if searched is None and 'google_search_key' in config.var_dict:
                searched = self.searcher.search(url, search_engine='google')
            end = time.time()
            self.tracer.info(f'Runtime (Search): {end - start}')
            update_dict = {}
            has_title = self.db.reorg.find_one({'url': url})
            if has_title is None:
                has_title = {'url': url, 'hostname': self.site}
                self.db.reorg.update_one({'url': url}, {'$set': has_title}, upsert=True)
            if 'title' not in has_title:
                try:
                    wayback_url = self.memo.wayback_index(url)
                    html = self.memo.crawl(wayback_url)
                    title = self.memo.extract_title(html, version='domdistiller')
                except: # No snapthost on wayback
                    self.tracer.error(f'WB_Error {url}: Fail to get data from wayback')
                    try:
                        self.db.na_urls.update_one({'_id': url}, {"$set": {
                            'url': url,
                            'hostname': self.site,
                            'no_snapshot': True
                        }}, upsert=True)
                    except: pass
            else:
                title = has_title['title']

            self.tracer.flush()

            if searched is not None:
                searched, trace = searched
                self.tracer.info(f"HIT: {searched}")
                update_dict.update({'reorg_url': searched, 'by':{
                    "method": "search"
                }})
                update_dict['by'].update(trace)

            try:
                self.db.reorg.update_one({'url': url}, {"$set": {self.classname: update_dict, "title": title}} ) 
            except Exception as e:
                self.tracer.warn(f'Search update DB: {str(e)}')

            # * Inference
            if infer and searched is not None:
                new_finds = self.infer_new(url, (title,), searched)
                broken_urls.difference_update(new_finds)
    
    def discover(self, required_urls, infer=False,):
        """
        infer: Infer every time when a new alias is found
        Required urls: URLs that will be run on
        """
        if self.similar.site is None or self.site not in self.similar.site:
            self.similar.clear_titles()
            if not self.similar._init_titles(self.site):
                self.tracer.warn(f"Similar._init_titles: Fail to get homepage of {self.site}")
                return

        reorg_checked = list(self.db.reorg.find({"hostname": self.site, self.classname: {"$exists": True}}))
        reorg_checked = set([u['url'] for u in reorg_checked])
        broken_urls = set([ru for ru in required_urls if ru not in reorg_checked])

        self.tracer.info(f'Discover SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.tracer.info(f'URL: {i} {url}')
            method, suffice = 'discover', False
            while True: # Dummy while lloop served as goto
                self.tracer.info("Start wayback alias")
                start = time.time()
                # ! TEMP
                # discovered = self.discoverer.wayback_alias(url)
                # if discovered:
                #     trace = {'suffice': True, 'type': 'wayback_alias', 'value': None}
                #     break
                # ! End TEMP

                # self.tracer.info("Start backpath (latest)")
                # discovered, trace = self.discoverer.bf_find(url, policy='latest')
                # if discovered:
                #     method = 'backpath_latest'
                #     break
                
                self.tracer.info("Start discover")
                discovered, trace = self.discoverer.discover(url)
                if discovered:
                    break
                suffice = trace['suffice']

                break
            
            end = time.time()
            self.tracer.info(f'Runtime (discover): {end - start}')
            self.tracer.flush()
            update_dict = {}
            has_title = self.db.reorg.find_one({'url': url})
            if has_title is None:
                has_title = {'url': url, 'hostname': self.site}
                self.db.reorg.update_one({'url': url}, {'$set': has_title}, upsert=True)
            # * Get title of the URL (if available)
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
                update_dict.update({'reorg_url': discovered, 'by':{
                    "method": method
                }})
                by_discover = {k: v for k, v in trace.items() if k not in ['trace', 'backpath', 'suffice']}
                update_dict['by'].update(by_discover)
            elif not suffice:
                try:
                    self.db.na_urls.update_one({'_id': url}, {'$set': {
                        'no_working_parent': True, 
                        'hostname': self.site
                    }}, upsert=True)
                except:pass

            # * Update dict correspondingly
            try:
                self.db.reorg.update_one({'url': url}, {'$set': {self.classname: update_dict, 'title': title}})
            except Exception as e:
                self.tracer.warn(f'Discover update DB: {str(e)}')
            
            # * Inference
            if infer and discovered is not None:
                new_finds = self.infer_new(url, (title,), discovered)
                broken_urls.difference_update(new_finds)
    
