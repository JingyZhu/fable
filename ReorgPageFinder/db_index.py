"""
Create Index for different collections used by ReorgPageFinder
"""
from pymongo import MongoClient
import pymongo

import sys
sys.path.append('../')
import config

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder

db.crawl.create_index([('html', pymongo.ASCENDING)])