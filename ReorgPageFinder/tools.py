"""
Functions for determines whether two pages are simiar/same
Methodologies: Content Match / Parital Match
"""
import pymongo
from pymongo import MongoClient
import brotli
import re

import sys
sys.path.append('../')
import config
from utils import text_utils, crawl

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder
class Memoize:
    """
    Class for reducing crawl and wayback indexing
    """
    def __init__(self, use_db=True, db=db):
        """
        # TODO: Implement non-db version. (In mem version)
        """
        self.use_db = db
        if use_db:
            self.db = db
    
    def crawl(self, url, **kwargs):
        html = self.db.crawl.find_one({'_id': url})
        if html: 
            return brotli.decompress(html['html']).decode()
        html = crawl.requests_crawl(url, **kwargs)
        try:
            self.db.crawl.insert_one({
                "_id": url,
                "url": url,
                "html": broti.compress(html.encode())
            })
        except:
            pass
        return html


class Similar:
    def __init__(self, use_db, db=db, corpus=[]):
        if not use_db and len(corpus) == 0:
            raise Exception("Corpus is requred for tfidf if db is not set")
        self.use_db = use_db
        self.threshold = 0.8
        if use_db:
            self.db =  db
            corpus = self.db.corpus.find({'$or': [{'src': 'realweb'}, {'usage': re.compile('represent')}]}, {'content': True})
            corpus = [c['content'] for c in list(corpus)]
            self.tfidf = text_utils.TFidfStatic(corpus)
        else:
            self.tfidf = text_utils.TFidfStatic(corpus)
    
    def content_similar(self, content1, content2):
        similarity = self.tfidf.similar(content1, content2)
        return similarity
    
