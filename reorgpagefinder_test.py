from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import config
from utils import text_utils, url_utils

sites = [
        # 'commonsensemedia.org', # Guess + similar link
        # 'filecart.com',  # Loop + similar link
        # 'imageworksllc.com',  
        # 'onlinepolicy.org',  # Guess + Content
        # 'mobilemarketingmagazine.com',  # Search + Content
        'planetc1.com', # Search
        # 'smartsheet.com'
]

rpf = ReorgPageFinder.ReorgPageFinder()
for site in sites:
    rpf.init_site(site, [])
#     rpf.infer()
    rpf.second_search()
