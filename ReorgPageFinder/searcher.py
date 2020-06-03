"""
Search broken pages' content
"""
import requests
from urllib.parse import urlparse 
from pymongo import MongoClient
import pymongo
import re

import sys
sys.path.append('../')
import config
from utils import search, crawl, text_utils, url_utils

class Searcher:
    def __init__(self, use_db=True, corpus=[], proxies={}):
        """
        At lease one of db or corpus should be provided
        # TODO: Corpus could not be necessary
        """
        self.PS = crawl.ProxySelector(proxies)
        if not use_db and len(corpus) == 0:
            raise Exception("Corpus is requred for tfidf if db is not set")
        self.use_db = use_db
        if use_db:
            self.db =  MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
            corpus = self.db.url_content.find({'$or': [{'src': 'realweb'}, {'usage': re.compile('represent')}]}, {'content': True})
            corpus = [c['content'] for c in list(corpus)]
            self.tfidf = text_utils.TFidfStatic(corpus)
        else:
            self.tfidf = text_utils.TFidfStatic(corpus)
    
    def search(self, url, wayback=False, search_engine='bing'):
        """wayback: Whether the url is snapshot on the wayback"""
        if search_engine not in ['google', 'bing']:
            raise Exception("Search engine could support for google and bing")
        search_results = []
        he = url_utils.HostExtractor()
        site = he.extract(url, wayback=wayback)
        if '://' not in site: site = f'http://{site}'
        r = requests.get(site, headers=crawl.requests_header, timeout=10)
        site = he.extract(r.url)
        html = crawl.requests_crawl(url, proxies=self.PS.select())
        title = search.get_title(html)
        content = text_utils.extract_body(html)
        self.tfidf._clear_workingset()
        topN = self.tfidf.topN(content)
        topN = ' '.join(topN)
        print(f'title: {title}')
        print(f'topN: {topN}')
        if title != '':
            if search_engine == 'google':
                search_results += search.google_search(f'"{title}"', use_db=self.use_db)
                search_results += search.google_search(f'{title}', site_spec_url=site)
            else:
                search_results += search.bing_search(f'+"{title}"', use_db=self.use_db)
                search_results += search.bing_search(f'{title} site:{site}', use_db=self.use_db)
        if len(topN) > 0:
            if search_engine == 'google':
                search_results += search.google_search(topN, site_spec_url=site, use_db=self.use_db)
            else:
                search_results += search.bing_search(f'{topN} site:{site}', use_db=self.use_db)
        search_results = list(set(search_results))
        print(search_results)
        searched_contents = {}
        for url in search_results:
            html = crawl.requests_crawl(url, proxies=self.PS.select())
            if html is None: continue
            searched_contents[url] = text_utils.extract_body(html)
        self.tfidf._clear_workingset()
        self.tfidf.add_corpus([content] + list(searched_contents.values()))
        searched_simi = {url: self.tfidf.similar(content, sc) for url, sc in searched_contents.items()}
        searched_simi = sorted(searched_simi.items(), key=lambda x: x[1], reverse=True)
        return searched_simi[0]
        