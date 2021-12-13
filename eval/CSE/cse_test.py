import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import sys
sys.path.append('../../')
from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder, ReorgPageFinder_deploy
import config
from utils import text_utils, url_utils


rpf = ReorgPageFinder_deploy.ReorgPageFinder(logname='./ReorgPageFinder_CSE.log')

site = 'umich.edu'
urls = json.load(open('Broken_CSE_add.json', 'r'))
rpf.init_site(site, urls)
rpf.infer()
# rpf.first_search()
rpf.second_search(required_urls=urls)
rpf.discover(required_urls=urls)
rpf.infer()
