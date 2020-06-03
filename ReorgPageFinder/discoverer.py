"""
Discover backlinks to today's page
"""
import os
from urllib.parse import urlsplit, urlparse, parse_qsl, urlunsplit
from itertools import chain, combinations
import bs4

import sys
sys.path.append('../')
import config
from utils import search, crawl, text_utils, url_utils

class Discoverer:
    def __init__(self, depth=5, corpus=[], proxies={}):
        self.depth = depth
        self.corpus = corpus
        self.PS = crawl.ProxySelector(proxies)
    
    def guess_backlinks(self, url):
        """
        Guess backlinks by returning:
            The parent url & If url with query, no query / partial query
        """
        def powerset(iterable):
            "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
            s = list(iterable)
            return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))
        us = urlsplit(url)
        path, query = us.path, us.query
        guessed_urls = []
        path_dir = os.path.dirname(path)
        us_tmp = us._replace(path=path_dir, query='')
        guessed_urls.append(urlunsplit(us_tmp))
        if not query:
            return guessed_urls
        qsl = parse_qsl(query)
        if len(qsl) == 0:
            us_tmp = us._replace(query='')
            guessed_urls.append(urlunsplit(us_tmp))
            return guessed_urls
        for sub_q in powerset(qsl):
            if len(sub_q) == len(qsl): continue
            us_tmp = us._replace(query='&'.join([f'{kv[0]}={kv[1]}' for kv in sub_q]))
            guessed_urls.append(urlunsplit(us_tmp))
        return guessed_urls
    
    