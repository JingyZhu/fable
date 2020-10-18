import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time
import json
import sys

sys.path.append('../')
from fable import config
from fable.utils import text_utils, url_utils
from fable import ReorgPageFinder

db_wd = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay

rpf = ReorgPageFinder(logname='./rpf.log')
urls = ['http://www.ftc.gov/os/highlights/2013/chairwomans-message.shtml']
rpf.init_site('ftc.gov', urls)
rpf.search(required_urls=urls)
rpf.discover(required_urls=urls)
