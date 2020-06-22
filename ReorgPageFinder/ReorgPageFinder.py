from ReorgPageFinder import discoverer, searcher, inferer, tools
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import config
from utils import text_utils, url_utils, crawl

db_broken = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder
he = url_utils.HostExtractor()

sites = [
        # 'commonsensemedia.org', # Guess + similar link
        # 'filecart.com',  # Loop + similar link
        # 'imageworksllc.com',  
        # 'onlinepolicy.org',  # Guess + Content
        # 'mobilemarketingmagazine.com',  # Search + Content
        # 'planetc1.com', # Search
        'smartsheet.com'
]

def get_dirr(url):
    us = urlsplit(url)
    if us.path[-1] == '/' and us.path != '/':
        us = us._replace(path=us.path[:-1])
    return (us.netloc, os.path.dirname(us.path))

def same_dirr(url1, url2):
    return get_dirr(url1) == get_dirr(url2)

class ReorgPageFinder:
    def __init__(self, use_db=True, db=db memo=None, similar=None, proxies={}, logger=None):
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.PS = crawl.ProxySelector(proxies)
        self.logger = logger if logger is not None else self._init_logger()
        self.searcher = searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.discoverer = discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
    
    def _init_logger(self):
        import logging
        logger = logging.getLogger('logger')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s %(asctime)s [%(filename)s %(funcName)s:%(lineno)s]: \n %(message)s')
        file_handler = logging.FileHandler('./ReorgPageFinder.log')
        file_handler.setFormatter(formatter)
        std_handler = logging.StreamHandler()
        std_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(std_handler)
        return logger
    
    def init_site(self, site, urls):
        self.site = site
        objs = [{'url': url, 'hostname': site} for url in urls]
        try:
            self.db.reorg.insert_many(objs, ordered=False)
        except: pass

    def first_search(self):
        noreorg_urls = db.reorg.find({"hostname": self.site, 'reorg_url': {"$exists": False}})
        searched_checked = db.checked.find({"hostname": self.site, "search_1": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        urls = [u in noreorg_urls if u['url'] not in searched_checked]
        for i, obj in enumerate(urls):
            url, hostname = obj['url'], obj['hostname']
            self.logger.info(f'URL: {i} {url}')
            searched = self.searcher.search(url, search_engine='bing')
            if searched is None:
                searched = self.searcher.search(url, search_engine='google')
            wayback_url = self.memo.wayback_index(url)
            html = self.memo.crawl(wayback_url)
            title = self.memo.extract_title(html, version='domdistiller')
            update_dict = {'title': title}
            if searched is not None:
                self.logger.info("HIT:", searched)
                update_dict.update({'reorg_url': searched, 'by': 'search'})
            try:
                self.db.reorg.update_one({'url': url}, {"$set": update_dict}) 
            except Exception as e:
                self.logger.warn(f'First search update DB: {str(e)}')
            searched_checked.add(url)
            try:
                self.db.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site 
                    "search_1": True
                }}, upsert=True)
            except: pass

    def second_search(self):
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        noreorg_urls = db.reorg.find({"hostname": self.site, 'reorg_url': {"$exists": False}})
        searched_checked = db.checked.find({"hostname": self.site, "search_2": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        urls = [u in noreorg_urls if u['url'] not in searched_checked]
        for i, obj in enumerate(urls):
            url, hostname = obj['url'], obj['hostname']
            self.logger.info(f'URL: {i} {url}')
            searched = self.searcher.search(url, search_engine='bing')
            if searched is None:
                searched = self.searcher.search(url, search_engine='google')
            wayback_url = self.memo.wayback_index(url)
            html = self.memo.crawl(wayback_url)
            title = self.memo.extract_title(html, version='domdistiller')
            update_dict = {'title': title}
            if searched is not None:
                self.logger.info("HIT:", searched)
                update_dict.update({'reorg_url': searched, 'by': 'search'})
            try:
                self.db.reorg.update_one({'url': url}, {"$set": update_dict}) 
            except Exception as e:
                self.logger.warn(f'Second search update DB: {str(e)}')
            searched_checked.add(url)
            try:
                self.db.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site 
                    "search_2": True
                }}, upsert=True)
            except: pass
    
    def discover(self):
        pass



def query_inferer(examples, site):
    """
    examples: (list) passed by referece

    Return urls that successfully infered. For callers' saving work
    """
    global ifr, db, memo
    if len(examples) <= 0:
        return []
    broken_urls = db.reorg.find({'hostname': site, 'reorg_url': {'$exists': False}})
    db.reorg.update_many({'hostname': site, "title": ""}, {"$unset": {"title": ""}})
    infer_urls = []
    for infer_url in list(broken_urls):
        if not same_dirr(infer_url['url'], examples[0][0][0]):
            continue
        if 'title' not in infer_url:
            try:
                wayback_infer_url = memo.wayback_index(infer_url['url'])
                wayback_infer_html = memo.crawl(wayback_infer_url)
                title = memo.extract_title(wayback_infer_html)
                db.reorg.update_one({'_id': infer_url['_id']}, {'$set': {'title': title}})
            except Exception as e:
                logger.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
                return []
        else: title = infer_url['title'] 
        infer_urls.append((infer_url['url'], (title)))
    if len(infer_urls) <=0:
        return []
    infered_dict = ifr.infer(examples, infer_urls, site=site + str(time.time()))
    # logger.info(f'example: {examples}')
    logger.info(f'infered_dict: {infered_dict}')
    infer_urls = {iu[0]: iu for iu in infer_urls}
    success = []
    for infer_url, cand in infered_dict.items():
        # logger.info(f'Infer url: {infer_url} {cand}')
        reorg_url, reason = ifr.if_reorg(infer_url, cand)
        if reorg_url is not None:
            db.reorg.update_one({'url': infer_url}, {'$set': {'reorg_url': reorg_url, 'by': 'infer'}})
            examples.append((infer_urls[infer_url], reorg_url))
            success.append(infer_url)
    return success

for site in sites:
    reorg_urls = db.reorg.find({'hostname': site, 'reorg_url': {'$exists': True}})
    reorg_urls = list(reorg_urls)
    reorg_dirs = defaultdict(list)
    for reorg_url in reorg_urls:
        dirr = get_dirr(reorg_url['url'])
        reorg_dirs[dirr].append(((reorg_url['url'], (reorg_url['title'])), reorg_url['reorg_url']))
    similar._init_titles(site=site)
    broken_urls = list(db.reorg.find({'hostname': site, 'reorg_url': {'$exists': False}}))
    logger.info(f'SITE: {site}#URLS: {len(broken_urls)}')
    broken_urls = set([bu['url'] for bu in broken_urls])
    for reorg_dirr, examples in reorg_dirs.items():
        success = query_inferer(examples, site)
        while len(success) > 0:
            logger.info(f'Success {success}')
            for s in success: broken_urls.discard(s)
            success = query_inferer(examples, site)

    while len(broken_urls) > 0:
        url = broken_urls.pop()
        logger.info(f'URL: {url}')
        reorg_url = dis.discover(url)
        if reorg_url is not None:
            logger.info(f'Found reorg: {reorg_url}')
            wayback_url = memo.wayback_index(url)
            html = memo.crawl(wayback_url)
            title = memo.extract_title(html, version='domdistiller')
            db.reorg.update_one({'url': url}, {'$set': {'reorg_url': reorg_url, 'by': 'discover', 'title': title}})
            dirr = get_dirr(url)
            reorg_dirs[dirr].append(((url, (title)), reorg_url))
            examples = reorg_dirs[dirr]
            success = query_inferer(examples, site)
            while len(success) > 0:
                logger.info(f'Success {success}')
                for s in success: broken_urls.discard(s)
                success = query_inferer(examples, site)
            
