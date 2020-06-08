"""
Search broken pages' content
"""
import requests
from urllib.parse import urlparse 
from pymongo import MongoClient
import pymongo
import re
from . import tools

import sys
sys.path.append('../')
import config
from utils import search, crawl, text_utils, url_utils

class Searcher:
    def __init__(self, use_db=True, proxies={}, memo=None, similar=None):
        """
        At lease one of db or corpus should be provided
        # TODO: Corpus could not be necessary
        """
        self.PS = crawl.ProxySelector(proxies)
        self.use_db = use_db
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar() 
    
    def search(self, url, wayback=False, search_engine='bing'):
        """
        wayback: Whether the url is snapshot on the wayback
        # TODO: Only run later query when previous found no results
        """
        # import time
        # begin = time.time()
        if search_engine not in ['google', 'bing']:
            raise Exception("Search engine could support for google and bing")
        search_results = []
        he = url_utils.HostExtractor()
        site = he.extract(url, wayback=wayback)
        if '://' not in site: site = f'http://{site}'
        r = requests.get(site, headers=crawl.requests_header, timeout=10)
        site = he.extract(r.url)
        if not wayback:
            url = self.memo.wayback_index(url)
        print(f'url: {url}')
        html = self.memo.crawl(url, proxies=self.PS.select())
        title = search.get_title(html)
        content = text_utils.extract_body(html)
        self.similar.tfidf._clear_workingset()
        topN = self.similar.tfidf.topN(content)
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
        
        # TODO: May move all comparison techniques to similar class
        similars = self.similar.search_similar(html, content, searched_contents)
        # print(f"time: {time.time()-begin}")
        if len(similars) > 0: 
            return similars[0]
        else:
            return None
        