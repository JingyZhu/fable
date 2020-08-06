from ReorgPageFinder import discoverer, searcher, inferer, tools
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time
import json
import logging

import config
from utils import text_utils, url_utils, crawl, sic_transit

db_broken = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder
he = url_utils.HostExtractor()


def unpack_ex(ex):
    (url, title), reorg = ex
    return url, title, reorg


def gen_path_pattern(url, dis=1):
    """
    Generate path patterns where all paths with same edit distance should follow
    # TODO: Currently only support edit distance of 1,  Could have larger dis
    """
    us = urlsplit(url)
    us = us._replace(netloc=us.netloc.split(':')[0])
    if us.path == '':
        us = us._replace(path='/')
    if us.path[-1] == '/' and us.path != '/':
        us = us._replace(path=us.path[:-1])
    path_lists = list(filter(lambda x: x!= '', us.path.split('/')))
    if us.query: 
        path_lists.append(us.query)
    patterns = []
    patterns.append(tuple(['*'] + path_lists))
    for i in range(len(path_lists)):
        path_copy = path_lists.copy()
        path_copy[i] = '*'
        patterns.append(tuple([us.netloc] + path_copy))
    return patterns


def pattern_match(pattern, url, dis=1):
    us = urlsplit(url)
    us = us._replace(netloc=us.netloc.split(':')[0])
    if us.path == '':
        us = us._replace(path='/')
    if us.path[-1] == '/' and us.path != '/':
        us = us._replace(path=us.path[:-1])
    path_lists = list(filter(lambda x: x!= '', us.path.split('/')))
    path_lists = [us.netloc] + path_lists
    if us.query: 
        path_lists.append(us.query)
    if len(pattern) != len(path_lists):
        return False
    for pat, path in zip(pattern, path_lists):
        if pat == '*': continue
        elif pat != path: return False
    return True


def path_edit_distance(url1, url2):
    us1, us2 = urlsplit(url1), urlsplit(url2)
    dis = 0
    if us1.netloc != us2.netloc:
        dis += 1
    for part1, part2 in zip(list(filter(lambda x: x!= '', us1.path.split('/'))), \
                            list(filter(lambda x: x!= '', us2.path.split('/')))):
        if part1 != part2: dis += 1
    return dis

class ReorgPageFinder:
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, logger=None, logname=None, trace=False):
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.PS = crawl.ProxySelector(proxies)
        self.searcher = searcher.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.discoverer = discoverer.Discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
        self.pattern_dict = None
        self.logname = './ReorgPageFinder.log' if logname is None else logname
        self.logger = logger if logger is not None else self._init_logger()
        self.trace = trace

    def _init_logger(self):
        logger = logging.getLogger('logger')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s %(asctime)s [%(filename)s %(funcName)s:%(lineno)s]: \n %(message)s')
        file_handler = logging.FileHandler(self.logname)
        file_handler.setFormatter(formatter)
        std_handler = logging.StreamHandler()
        std_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(std_handler)
        return logger
    
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
        reorg_urls = self.db.reorg.find({'hostname': site, 'reorg_url': {"$exists": True}})
        self.pattern_dict = defaultdict(list)
        for reorg_url in list(reorg_urls):
            # Patch the no title urls
            if 'title' not in reorg_url:
                wayback_reorg_url = self.memo.wayback_index(reorg_url['url'])
                reorg_html, wayback_reorg_url = self.memo.crawl(wayback_reorg_url, final_url=True)
                reorg_title = self.memo.extract_title(reorg_html, version='domdistiller')
                reorg_url['title'] = reorg_title
                self.db.reorg.update_one({'url': reorg_url['url']}, {'$set': {'title': reorg_title}})
            self._add_url_to_patterns(reorg_url['url'], reorg_url['title'], reorg_url['reorg_url'])
        if len(self.logger.handlers) > 2:
            self.logger.handlers.pop()
        formatter = logging.Formatter('%(levelname)s %(asctime)s [%(filename)s %(funcName)s:%(lineno)s]: \n %(message)s')
        file_handler = logging.FileHandler(f'./logs/{site}.log')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def clear_site(self):
        self.site = None
        self.pattern_dict = None
        self.logger.handlers.pop()

    def _add_url_to_patterns(self, url, title, reorg):
        """
        Only applies to same domain currently
        Return bool on whether success
        """
        if he.extract(reorg) != he.extract(url):
            return False
        patterns = gen_path_pattern(url)
        if len(patterns) <= 0: return False
        for pat in patterns:
            self.pattern_dict[pat].append(((url, title), reorg))
        return True

    def query_inferer(self, examples):
        """
        examples: Lists of (url, title), reorg_url. 
                Should already be inserted into self.pattern_dict
        """
        if len(examples) <= 0:
            return []
        patterns = set()
        for (url, title), reorg_url in examples:
            pats = gen_path_pattern(url)
            patterns.update(pats)
        patterns = list(patterns)
        broken_urls = self.db.reorg.find({'hostname': self.site, 'reorg_url': {'$exists': False}})
        self.db.reorg.update_many({'hostname': self.site, "title": ""}, {"$unset": {"title": ""}})
        infer_urls = defaultdict(list) # Pattern: urls
        for infer_url in list(broken_urls):
            for pat in patterns:
                if not pattern_match(pat, infer_url['url']):
                    continue
                if 'title' not in infer_url:
                    try:
                        wayback_infer_url = self.memo.wayback_index(infer_url['url'])
                        wayback_infer_html = self.memo.crawl(wayback_infer_url)
                        title = self.memo.extract_title(wayback_infer_html)
                        self.db.reorg.update_one({'_id': infer_url['_id']}, {'$set': {'title': title}})
                    except Exception as e:
                        self.logger.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
                        title = ""
                else: title = infer_url['title'] 
                infer_urls[pat].append((infer_url['url'], (title)))
        if len(infer_urls) <=0:
            return []
        success = []
        for pat, pat_urls in infer_urls.items():
            infered_dict = self.inferer.infer(self.pattern_dict[pat], pat_urls, site=self.site)
            self.logger.info(f'infered_dict: {json.dumps(infered_dict, indent=4)}')
            pat_infer_urls = {iu[0]: iu for iu in infer_urls[pat]}
            for infer_url, cand in infered_dict.items():
                # logger.info(f'Infer url: {infer_url} {cand}')
                reorg_url, trace = self.inferer.if_reorg(infer_url, cand)
                if reorg_url is not None:
                    self.logger.info(f'Found by infer: {infer_url} --> {reorg_url}')
                    if not url_utils.url_match(infer_url, reorg_url):
                        by_dict = {'method': 'infer'}
                        by_dict.update(trace)
                        self.db.reorg.update_one({'url': infer_url}, {'$set': {
                            'reorg_url': reorg_url, 
                            'by': by_dict
                        }})
                        suc = ((pat_infer_urls[infer_url]), reorg_url)
                        self._add_url_to_patterns(*unpack_ex(suc))
                        success.append(suc)
                    else: # False positive
                        try: self.db.na_urls.update_one({'_id': infer_url}, {'$set': {
                                'false_positive': True,
                                'hostname': self.site
                            }}, upsert=True)
                        except: pass
        return success

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

    def infer(self):
        urls = {}
        success = [None]
        for examples in self.pattern_dict.values():
            for example in examples:
                (url, _), _ = example
                urls[url] = example
        examples = list(urls.values())
        success = list(urls.values())
        while(len(success)) > 0:
            success = self.query_inferer(examples)
            for suc in success:
                self._add_url_to_patterns(*unpack_ex(suc))
            examples = success

    def first_search(self):
        # _search
        noreorg_urls = db.reorg.find({"hostname": self.site, 'reorg_url': {"$exists": False}})
        searched_checked = db.checked.find({"hostname": self.site, "search_1": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        urls = [u for u in noreorg_urls if u['url'] not in searched_checked ]
        broken_urls = set([u['url'] for u in urls])
        self.logger.info(f'Search1 SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        self.similar.clear_titles()
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            searched = self.searcher.search(url, search_engine='bing')
            if searched is None:
                searched = self.searcher.search(url, search_engine='google')
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
                continue


            update_dict = {'title': title}
            if searched is not None:
                searched, trace = searched
                self.logger.info(f"HIT_1: {searched}")
                fp = self.fp_check(url, searched)
                if not fp: # False positive test
                    # _search
                    update_dict.update({'reorg_url': searched, 'by':{
                        "method": "search"
                    }})
                    update_dict['by'].update(trace)
                else:
                    try: self.db.na_urls.update_one({'_id': url}, {'$set': {
                            'url': url,
                            'false_positive_search': True, 
                            'hostname': self.site
                        }}, upsert=True)
                    except: pass
                    searched = None


            try:
                self.db.reorg.update_one({'url': url}, {"$set": update_dict}) 
            except Exception as e:
                self.logger.warn(f'First search update DB: {str(e)}')
            searched_checked.add(url)
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    "search_1": True
                }}, upsert=True)
            except Exception as e:
                self.logger.warn(f'Discover update checked: {str(e)}')
            if searched is not None:
                example = ((url, title), searched)
                added = self._add_url_to_patterns(*unpack_ex(example))
                if not added: 
                    continue
                success = self.query_inferer([example])
                while len(success) > 0:
                    added = False
                    for suc in success:
                        broken_urls.discard(unpack_ex(suc)[0])
                        a = self._add_url_to_patterns(*unpack_ex(suc))
                        added = added or a
                    if not added: 
                        break 
                    examples = success
                    success = self.query_inferer(examples)

    def second_search(self):
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        # _search
        noreorg_urls = self.db.reorg.find({"hostname": self.site, 'reorg_url': {"$exists": False}})
        searched_checked = self.db.checked.find({"hostname": self.site, "search_2": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        urls = [u for u in noreorg_urls if u['url'] not in searched_checked ]
        broken_urls = set([u['url'] for u in urls])
        self.logger.info(f'Search2 SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            searched = self.searcher.search(url, search_engine='bing')
            if searched is None:
                searched = self.searcher.search(url, search_engine='google')
            update_dict = {}
            has_title = self.db.reorg.find_one({'url': url})
            # if has_title is None: # No longer in reorg (already deleted)
            #     continue
            if 'title' not in has_title or has_title['title'] == 'N/A':
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
                    continue
                update_dict = {'title': title}
            else:
                title = has_title['title']


            if searched is not None:
                searched, trace = searched
                self.logger.info(f"HIT_2: {searched}")
                fp = self.fp_check(url, searched)
                if not fp: # False positive test
                    # _search
                    update_dict.update({'reorg_url': searched, 'by':{
                        "method": "search"
                    }})
                    update_dict['by'].update(trace)
                else:
                    try: self.db.na_urls.update_one({'_id': url}, {'$set': {
                            'url': url,
                            'false_positive_search': True, 
                            'hostname': self.site
                        }}, upsert=True)
                    except: pass
                    searched = None


            if len(update_dict) > 0:
                try:
                    self.db.reorg.update_one({'url': url}, {"$set": update_dict}) 
                except Exception as e:
                    self.logger.warn(f'Second search update DB: {str(e)}')
            searched_checked.add(url)
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    "search_2": True
                }}, upsert=True)
            except: pass
            if searched is not None:
                example = ((url, title), searched)
                added = self._add_url_to_patterns(*unpack_ex(example))
                if not added: 
                    continue
                success = self.query_inferer([example])
                while len(success) > 0:
                    added = False
                    for suc in success:
                        broken_urls.discard(unpack_ex(suc)[0])
                        a = self._add_url_to_patterns(*unpack_ex(suc))
                        added = added or a
                    if not added: 
                        break
                    examples = success
                    success = self.query_inferer(examples)
    
    def discover(self):
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        # _discover
        noreorg_urls = self.db.reorg.find({"hostname": self.site, 'reorg_url': {"$exists": False}})
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
            discovered, trace = self.discoverer.discover(url)
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
                    update_dict.update({'reorg_url': discovered, 'by':{
                        "method": "discover"
                    }})
                    by_discover = {k: v for k, v in trace.items() if k != 'trace'}
                    update_dict['by'].update(by_discover)
                else:
                    try: self.db.na_urls.update_one({'_id': url}, {'$set': {
                            'url': url,
                            'false_positive_discover': True, 
                            'hostname': self.site
                        }}, upsert=True)
                    except: pass
                    discovered = None
            elif not trace['suffice']:
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
            if self.trace:
                try:
                    self.db.trace.update_one({'_id': url}, {"$set": {
                        "url": url,
                        "hostname": self.site,
                        "discover": trace['trace']
                    }}, upsert=True)
                except Exception as e:
                    self.logger.warn(f'Discover update trace: {str(e)}')
            
            # TEMP
            if discovered is not None:
                example = ((url, title), discovered)
                added = self._add_url_to_patterns(*unpack_ex(example))
                if not added:
                    continue
                success = self.query_inferer([example])
                while len(success) > 0:
                    added = False
                    for suc in success:
                        broken_urls.discard(unpack_ex(suc)[0])
                        a = self._add_url_to_patterns(*unpack_ex(suc))
                        added = added or a
                    if not added:
                        break
                    examples = success
                    success = self.query_inferer(examples)
