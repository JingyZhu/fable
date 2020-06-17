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

import logging
logger = logging.getLogger('logger')

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
        if search_engine not in ['google', 'bing']:
            raise Exception("Search engine could support for google and bing")
        he = url_utils.HostExtractor()
        site = he.extract(url, wayback=wayback)
        if '://' not in site: site = f'http://{site}'
        _, final_url = self.memo.crawl(site, final_url=True)
        if final_url is None: return
        site = he.extract(final_url)
        if not wayback:
            url = self.memo.wayback_index(url)
        if url is None:
            return
        html = self.memo.crawl(url, proxies=self.PS.select())
        if html is None:
            return
        title = search.get_title(html)
        content = self.memo.extract_content(html)
        logger.info(f'title: {title}')
        search_results, searched = [], set()

        def search_once(search_results):
            """Incremental Search"""
            nonlocal url, title, content, html, searched
            he = url_utils.HostExtractor()
            searched_contents = {}
            searched_titles = {}
            search_cand = [s for s in search_results if s not in searched]
            logger.info(f'#Search cands: {len(search_cand)}')
            searched.update(search_results)
            for searched_url in search_cand:
                searched_html = self.memo.crawl(url, proxies=self.PS.select())
                if searched_html is None: continue
                searched_contents[searched_url] = self.memo.extract_content(searched_html)
                if he.extract(url) == he.extract(searched_url):
                    searched_titles[searched_url] = self.memo.extract_title(searched_html)
        
            # TODO: May move all comparison techniques to similar class
            similars = self.similar.similar(url, title, content, searched_titles, searched_contents)
            if len(similars) > 0: 
                return similars[0]
            
            return

        if title != '':
            if search_engine == 'google':
                search_results = search.google_search(f'"{title}"', use_db=self.use_db)
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
                search_results = search.google_search(f'{title}', site_spec_url=site)
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
            else:
                search_results = search.bing_search(f'+"{title}"', use_db=self.use_db)
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
                if site is not None:
                    site_str = f'site:{site}'
                else:
                    site_str = ''
                search_results = search.bing_search(f'{title} {site_str}', use_db=self.use_db)
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
        
        self.similar.tfidf._clear_workingset()
        topN = self.similar.tfidf.topN(content)
        topN = ' '.join(topN)
        logger.info(f'topN: {topN}')
        if len(topN) > 0:
            if search_engine == 'google':
                search_results = search.google_search(topN, site_spec_url=site, use_db=self.use_db)
                similar = search_once(search_results)
                if similar is not None:
                    return similar
            else:
                if site is not None:
                    site_str = f'site:{site}'
                else: 
                    site_str = ''
                search_results = search.bing_search(f'{topN} {site_str}', use_db=self.use_db)
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
        return