"""
Create Index for different collections used by ReorgPageFinder
"""
from pymongo import MongoClient
import pymongo

import sys
sys.path.append('../')
from . import config

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').fable

db.crawl.create_index([('html', pymongo.ASCENDING)])

db.searched.create_index([('query', pymongo.ASCENDING), ('engine', pymongo.ASCENDING)])

db.wayback_rep.create_index([('url', pymongo.ASCENDING), ('policy', pymongo.ASCENDING)], unique=True)