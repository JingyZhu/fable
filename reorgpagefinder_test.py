from ReorgPageFinder import discoverer, searcher, inferer, tools
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import config
from utils import text_utils, url_utils

import logging
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s %(asctime)s [%(filename)s %(funcName)s:%(lineno)s]: \n %(message)s')
file_handler = logging.FileHandler('ReorgPageFinder.log')
file_handler.setFormatter(formatter)
std_handler = logging.StreamHandler()
std_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(std_handler)

db_broken = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder
he = url_utils.HostExtractor()

sites = ['commonsensemedia.org', # Guess + similar link
        'filecart.com',  # Loop + similar link
        # 'imageworksllc.com',  
        # 'mobilemarketingmagazine.com',  # Search + Content
        'onlinepolicy.org',  # Guess + Content
        # 'planetc1.com', # Search
        # 'smartsheet.com'
]

# for site in sites:
#     site_urls = db_broken.url_status_implicit_broken.find({'hostname': site, "$or": [{"broken": True}, {"sic_broken": True}]})
#     urls += list(site_urls)

# urls = [(u['url'], u['hostname']) for u in urls]
# print(len(urls))

memo = tools.Memoizer()
# similar = tools.Similar(use_db=False, corpus=['This is a good day!', "Go blue!"])
similar = tools.Similar()

# se = searcher.Searcher(memo=memo, similar=similar)
# for i, (url, hostname) in enumerate(urls):
#     print('URL:', i, url)
#     reorg = db.reorg.find_one({'url': url})
#     if reorg is not None:
#         continue
#     searched = se.search(url, search_engine='bing')
#     if searched is None:
#         searched = se.search(url, search_engine='google')
#     if searched is not None:
#         print("HIT:", searched)
#         wayback_url = memo.wayback_index(url)
#         html = memo.crawl(wayback_url)
#         title = memo.extract_title(html, version='domdistiller')
#         try:
#             db.reorg.insert_one({
#                 'url': url,
#                 'hostname': hostname,
#                 'reorg_url': searched,
#                 'title': title
#             })
#         except Exception as e:
#             print(str(e))
#     else:
#         try:
#             db.reorg.insert_one({
#                 'url': url,
#                 'hostname': hostname
#             })
#         except: pass

def get_dirr(url):
    us = urlsplit(url)
    return (us.netloc, os.path.dirname(us.path))

def same_dirr(url1, url2):
    return get_dirr(url1) == get_dirr(url2)


dis = discoverer.Discoverer(memo=memo, similar=similar)
ifr = inferer.Inferer(memo=memo, similar=similar)

def query_inferer(examples, site):
    """
    examples: (list) passed by referece

    Return urls that successfully infered. For callers' saving work
    """
    global ifr, db, memo
    if len(examples) <= 0:
        return []
    broken_urls = db.reorg.find({'hostname': site, 'reorg_url': {'$exists': False}})
    infer_urls = []
    for infer_url in list(broken_urls):
        if not same_dirr(infer_url['url'], examples[0][0][0]):
            continue
        if 'title' not in infer_url:
            wayback_infer_url = memo.wayback_index(infer_url['url'])
            wayback_infer_html = memo.crawl(wayback_infer_url)
            title = memo.extract_title(wayback_infer_html)
            db.reorg.update_one({'_id': infer_url['_id']}, {'$set': {'title': title}})
        else: title = infer_url['title'] 
        infer_urls.append((infer_url['url'], (title)))
    if len(infer_urls) <=0:
        return []
    infered_dict = ifr.infer(examples, infer_urls, site=site + str(time.time()))
    infer_urls = {iu[0]: iu for iu in infer_urls}
    success = []
    for infer_url, cand in infered_dict.items():
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
            print('Success', success)
            for s in success: broken_urls.discard(s)
            success = query_inferer(examples, site)

    while len(broken_urls) > 0:
        url = broken_urls.pop()
        logger.info(f'URL: {url}')
        reorg_url = dis.discover(url)
        if reorg_url is not None:
            logger.info(f'Found reorg: {reorg_url}')
            db.reorg.update_one({'url': url}, {'$set': {'reorg_url': reorg_url, 'by': 'discover'}})
            dirr = get_dirr(url)
            examples = reorg_dirs[dirr]
            success = query_inferer(examples, site)
            while len(success) > 0:
                logger.info(f'Success {success}')
                for s in success: broken_urls.discard(s)
                success = query_inferer(examples, site)
            
