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


rpf = ReorgPageFinder(logname='./rpf.log')
urls = ['http://www.ftc.gov/os/highlights/2013/chairwomans-message.shtml']
rpf.init_site('ftc.gov', urls)
rpf.search(required_urls=urls)
rpf.discover(required_urls=urls)
