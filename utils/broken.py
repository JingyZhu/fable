"""
    Refined techniques for broken urls detection
    Optimized on:
        Determine whether wayback's crawl is a bad one
        Try to find factor on determining the routing of the page, both for wayback and liveweb
        Improved method for sic transit
"""
import requests
import re
import os
from urllib.parse import urlparse
import random, string
import sys
from pymongo import MongoClient

sys.path.append('../')
import config
from utils import sic_transit, text_utils

class BrokenClassifier:
    def __init__(self, corpus=None):
        """
        corpus: Corpus for tf-idf comparison, should be list of str. If none, default is used
        """
        self.db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
        if corpus:
            self.tfidf = text_utils.TFidf(corpus)
        else:
            corpus = self.db.url_content.find({'$or': [{'src': 'realweb'}, {'usage': re.compile('represent')}]}, {'content': True})
            corpus = [c['content'] for c in list(corpus)]
            self.tfidf = text_utils.TFidf(corpus)
    
    def broken(self, url, use_db=True):
        """
        Other than sic transit's way, also check for content similarity
        """
        content_simi = False
        up = urlparse(url)
        if use_db:
            url_match = f'{up.netloc.split(':')[0]}.*{up.path}{up.query}'
            urls = self.db.url_implicit_broken.find({'_id': re.compile(url_match)})
        
