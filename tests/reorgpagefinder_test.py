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


urls = ['http://parkweb.vic.gov.au/safety/fire,-flood-and-other-closures']
rpf = ReorgPageFinder(logname='coverage-imp')
rpf.init_site('parkweb.vic.gov.au', urls)
rpf.search(required_urls=urls)
# rpf.discover(required_urls=urls)
