"""
Create Index for different collections used by ReorgPageFinder
"""
from pymongo import MongoClient
import pymongo

from fable import config

db = config.DB

db.crawl.create_index([('html', pymongo.ASCENDING)])

db.searched.create_index([('query', pymongo.ASCENDING), ('engine', pymongo.ASCENDING)])

db.wayback_rep.create_index([('url', pymongo.ASCENDING), ('policy', pymongo.ASCENDING)], unique=True)