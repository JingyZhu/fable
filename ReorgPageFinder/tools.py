"""
Functions for determines whether two pages are simiar/same
Methodologies: Content Match / Parital Match
"""
import pymongo
from pymongo import MongoClient
import brotli
import re
from collections import defaultdict

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
    
    def match_url_sig(self, wayback_sig, liveweb_sigs):
        """
        See whether there is a url signature on liveweb that can match wayback sig
        Based on 2 methods: UNIQUE Similar anchor text, Non-UNIQUE same anchor text & similar sig
        """
        self.tfidf._clear_workingset()
        anchor_count = defaultdict(int)
        corpus = []
        for link, anchor, sig in [wayback_sig] + liveweb_sigs:
            anchor_count[(link, anchor)] += 1
            corpus.append(anchor)
            for s in sig:
                if s != '': corpus.append(s)
        self.tfidf.add_corpus(corpus)
        for lws in liveweb_sigs:
            link, anchor, sig = lws
            if anchor_count[(link, anchor)] < 2: # UNIQUE anchor
                simi = self.tfidf.similar(wayback_sig[1], anchor)
                if simi >= self.threshold:
                    return lws
            else:
                if wayback_sig[1] != anchor:
                    continue
                simi = 0
                for ws in wayback_sig[2]:
                    for ls in sig:
                        simi = max(simi, self.tfidf.simiar(ws, ls))
                if simi >= self.threshold:
                    return lws
        return None
    
    def search_similar(self, target, candidates):
        """
        See whether there are content from candidates that is similar target

        Return a list with all candidate higher than threshold
        """
        self.tfidf._clear_workingset()
        self.tfidf.add_corpus([target] + candidates.values())
        simi_cand = []
        for url, c in candidates.items():
            simi = self.tfidf.similar(target, c)
            if simi >= self.threshold:
                simi_cand.append((simi, url))
        return sorted(simi_cand, reverse=True)