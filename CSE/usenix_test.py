import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time

import sys
sys.path.append('../')
from ReorgPageFinder import discoverer, searcher, inferer, tools, ReorgPageFinder
import config
from utils import text_utils, url_utils


rpf = ReorgPageFinder.ReorgPageFinder(logname='./ReorgPageFinder_usenix.log')

site = 'usenix.org'
urls = json.load(open('Broken_usenix.json', 'r'))
rpf.init_site(site, urls)
rpf.infer()
rpf.first_search()
rpf.second_search()
rpf.discover()
rpf.infer()
