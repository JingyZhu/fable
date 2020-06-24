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

def same_format(url1, url2):
    us1, us2 = urlsplit(url1), urlsplit(url2)
    if us1.path[-1] == '/' and us1.path != '/':
        us1 = us1._replace(path=us1.path[:-1])
    if us2.path[-1] == '/' and us2.path != '/':
        us2 = us2._replace(path=us2.path[:-1])


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
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, logger=None):
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.PS = crawl.ProxySelector(proxies)
        self.logger = logger if logger is not None else self._init_logger()
        self.searcher = searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.discoverer = discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
        self.pattern_dict = None
    
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
        reorg_urls = self.db.reorg.find({'hostname': site, 'reorg_url': {"$exists": True}})
        self.pattern_dict = defaultdict(list)
        for reorg_url in list(reorg_urls):
            self._add_url_to_patterns(reorg_url['url'], reorg_url['title'], reorg_url['reorg_url'])

    def _add_url_to_patterns(self, url, title, reorg):
        patterns = gen_path_pattern(url)
        for pat in patterns:
            self.pattern_dict[pat].append(((url, title), reorg))

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
                if not pattern_match(pat, infer_url['infer_url']):
                    continue
                if 'title' not in infer_url:
                    try:
                        wayback_infer_url = self.memo.wayback_index(infer_url['url'])
                        wayback_infer_html = self.memo.crawl(wayback_infer_url)
                        title = self.memo.extract_title(wayback_infer_html)
                        self.db.reorg.update_one({'_id': infer_url['_id']}, {'$set': {'title': title}})
                    except Exception as e:
                        self.logger.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
                        return []
                else: title = infer_url['title'] 
                infer_urls[pat].append((infer_url['url'], (title)))
        if len(infer_urls) <=0:
            return []
        success = []
        for pat, pat_urls in infer_urls.items():
            infered_dict = self.inferer.infer(self.pattern_dict[pat], pat_urls, site=self.site + str(time.time()))
            self.logger.info(f'infered_dict: {infered_dict}')
            pat_infer_urls = {iu[0]: iu for iu in infer_urls[pat]}
            for infer_url, cand in infered_dict.items():
                # logger.info(f'Infer url: {infer_url} {cand}')
                reorg_url, reason = self.inferer.if_reorg(infer_url, cand)
                if reorg_url is not None:
                    self.db.reorg.update_one({'url': infer_url}, {'$set': {'reorg_url': reorg_url, 'by': 'infer'}})
                    self._add_url_to_patterns(*pat_infer_urls[infer_url])
                    success.append(infer_url)
        return success

    def first_search(self):
        noreorg_urls = db.reorg.find({"hostname": self.site, 'reorg_url': {"$exists": False}})
        searched_checked = db.checked.find({"hostname": self.site, "search_1": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        urls = [u for u in noreorg_urls if u['url'] not in searched_checked ]
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
                self.logger.info(f"HIT: {searched}")
                update_dict.update({'reorg_url': searched, 'by': 'search'})
            try:
                self.db.reorg.update_one({'url': url}, {"$set": update_dict}) 
            except Exception as e:
                self.logger.warn(f'First search update DB: {str(e)}')
            searched_checked.add(url)
            try:
                self.db.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
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
        urls = [u for u in noreorg_urls if u['url'] not in searched_checked]
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
                self.logger.info(f"HIT: {searched}")
                update_dict.update({'reorg_url': searched, 'by': 'search'})
            try:
                self.db.reorg.update_one({'url': url}, {"$set": update_dict}) 
            except Exception as e:
                self.logger.warn(f'Second search update DB: {str(e)}')
            searched_checked.add(url)
            try:
                self.db.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    "search_2": True
                }}, upsert=True)
            except: pass
    
    def discover(self):
        pass



# def query_inferer(examples, site):
#     """
#     examples: (list) passed by referece

#     Return urls that successfully infered. For callers' saving work
#     """
#     global ifr, db, memo
#     if len(examples) <= 0:
#         return []
#     broken_urls = db.reorg.find({'hostname': site, 'reorg_url': {'$exists': False}})
#     db.reorg.update_many({'hostname': site, "title": ""}, {"$unset": {"title": ""}})
#     infer_urls = []
#     for infer_url in list(broken_urls):
#         if not same_dirr(infer_url['url'], examples[0][0][0]):
#             continue
#         if 'title' not in infer_url:
#             try:
#                 wayback_infer_url = memo.wayback_index(infer_url['url'])
#                 wayback_infer_html = memo.crawl(wayback_infer_url)
#                 title = memo.extract_title(wayback_infer_html)
#                 db.reorg.update_one({'_id': infer_url['_id']}, {'$set': {'title': title}})
#             except Exception as e:
#                 logger.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
#                 return []
#         else: title = infer_url['title'] 
#         infer_urls.append((infer_url['url'], (title)))
#     if len(infer_urls) <=0:
#         return []
#     infered_dict = ifr.infer(examples, infer_urls, site=site + str(time.time()))
#     # logger.info(f'example: {examples}')
#     logger.info(f'infered_dict: {infered_dict}')
#     infer_urls = {iu[0]: iu for iu in infer_urls}
#     success = []
#     for infer_url, cand in infered_dict.items():
#         # logger.info(f'Infer url: {infer_url} {cand}')
#         reorg_url, reason = ifr.if_reorg(infer_url, cand)
#         if reorg_url is not None:
#             db.reorg.update_one({'url': infer_url}, {'$set': {'reorg_url': reorg_url, 'by': 'infer'}})
#             examples.append((infer_urls[infer_url], reorg_url))
#             success.append(infer_url)
#     return success

# for site in sites:
#     reorg_urls = db.reorg.find({'hostname': site, 'reorg_url': {'$exists': True}})
#     reorg_urls = list(reorg_urls)
#     reorg_dirs = defaultdict(list)
#     for reorg_url in reorg_urls:
#         dirr = get_dirr(reorg_url['url'])
#         reorg_dirs[dirr].append(((reorg_url['url'], (reorg_url['title'])), reorg_url['reorg_url']))
#     similar._init_titles(site=site)
#     broken_urls = list(db.reorg.find({'hostname': site, 'reorg_url': {'$exists': False}}))
#     logger.info(f'SITE: {site}#URLS: {len(broken_urls)}')
#     broken_urls = set([bu['url'] for bu in broken_urls])
#     for reorg_dirr, examples in reorg_dirs.items():
#         success = query_inferer(examples, site)
#         while len(success) > 0:
#             logger.info(f'Success {success}')
#             for s in success: broken_urls.discard(s)
#             success = query_inferer(examples, site)

#     while len(broken_urls) > 0:
#         url = broken_urls.pop()
#         logger.info(f'URL: {url}')
#         reorg_url = dis.discover(url)
#         if reorg_url is not None:
#             logger.info(f'Found reorg: {reorg_url}')
#             wayback_url = memo.wayback_index(url)
#             html = memo.crawl(wayback_url)
#             title = memo.extract_title(html, version='domdistiller')
#             db.reorg.update_one({'url': url}, {'$set': {'reorg_url': reorg_url, 'by': 'discover', 'title': title}})
#             dirr = get_dirr(url)
#             reorg_dirs[dirr].append(((url, (title)), reorg_url))
#             examples = reorg_dirs[dirr]
#             success = query_inferer(examples, site)
#             while len(success) > 0:
#                 logger.info(f'Success {success}')
#                 for s in success: broken_urls.discard(s)
#                 success = query_inferer(examples, site)
            
