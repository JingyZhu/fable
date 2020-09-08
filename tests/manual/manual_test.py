import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import sys
sys.path.append('../../')
from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder_deploy
import config
from utils import text_utils, url_utils


rpfd = ReorgPageFinder_deploy.ReorgPageFinder(logname='./ReorgPageFinder_ebates.log')

site = 'ebates.com'
urls = json.load(open('ebates.com.json', 'r'))
rpfd.init_site(site, urls)
# rpfd.infer()
# rpfd.first_search(infer=True)
rpfd.second_search(infer=True)
rpfd.discover(infer=True)
rpfd.infer()
