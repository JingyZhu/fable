from ReorgPageFinder import discoverer, searcher, tools
import pymongo
from pymongo import MongoClient

import config
from utils import text_utils

db_broken = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder

sites = ['commonsensemedia.org', # Guess + similar link
        'filecart.com',  # Loop + similar link
        # 'imageworksllc.com',  
        # 'mobilemarketingmagazine.com',  # Search + Content
        'onlinepolicy.org',  # Guess + Content
        'planetc1.com', # Search
        'smartsheet.com'
]

urls = []
# for site in sites:
#     site_urls = db_broken.url_status_implicit_broken.find({'hostname': site, "$or": [{"broken": True}, {"sic_broken": True}]})
#     urls += list(site_urls)

# urls = [(u['url'], u['hostname']) for u in urls]
# print(len(urls))

# memo = tools.Memoizer()
# similar = tools.Similar()

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

for site in sites:
    site_urls = db.reorg.find({'reorg_url': {'$exists': False}, 'hostname': site})
    urls += list(site_urls)
urls = [(u['url'], u['hostname']) for u in urls]
print(len(urls))

# dis = discoverer.Discoverer(memo=memo, similar=similar)