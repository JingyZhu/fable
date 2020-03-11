"""
Inspect temporal status of pages.
"""
import sys
from pymongo import MongoClient
import pymongo
import json, yaml
import socket
import re
from urllib.parse import urlparse
from collections import defaultdict, Counter
import requests
import json

sys.path.append('../')
import config
from utils import url_utils, crawl

db = MongoClient(config.MONGO_HOSTNAME).web_decay
db_test = MongoClient(config.MONGO_HOSTNAME).wd_test

PS = crawl.ProxySelector(config.PROXIES)

def dir_broken_ts