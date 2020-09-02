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
he = url_utils.HostExtractor()
class Searcher:
    def __init__(self, use_db=True, proxies={}, memo=None, similar=None):
        """
        At lease one of db or corpus should be provided
        # TODO: Corpus could not be necessary

        Return: 
            If found: URL, Trace (how copy is found, etc)
            else: None
        """
        self.PS = crawl.ProxySelector(proxies)
        self.use_db = use_db
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar() 

    def search(self, url, search_engine='bing'):
        global he
        if search_engine not in ['google', 'bing']:
            raise Exception("Search engine could support for google and bing")
        site = he.extract(url)
        if '://' not in site: site = f'http://{site}'
        _, final_url = self.memo.crawl(site, final_url=True)
        if final_url is not None:
            site = he.extract(final_url)
        try:
            wayback_url = self.memo.wayback_index(url)
            html = self.memo.crawl(wayback_url, proxies=self.PS.select())
            title = self.memo.extract_title(html, version='domdistiller')
            content = self.memo.extract_content(html)
        except Exception as e:
            logger.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
            return
        logger.info(f'title: {title}')
        search_results, searched = [], set()

        def search_once(search_results):
            """Incremental Search"""
            global he
            nonlocal url, title, content, html, searched
            searched_contents = {}
            searched_titles = {}
            search_cand = [s for s in search_results if s not in searched]
            logger.info(f'#Search cands: {search_cand}')
            searched.update(search_results)
            for searched_url in search_cand:
                searched_html = self.memo.crawl(searched_url, proxies=self.PS.select())
                logger.debug(f'Crawl: {searched_url}')
                if searched_html is None: continue
                searched_contents[searched_url] = self.memo.extract_content(searched_html)
                logger.debug(f'Extract Content: {searched_url}')
                if he.extract(url) == he.extract(searched_url) or site == he.extract(searched_url):
                    searched_titles[searched_url] = self.memo.extract_title(searched_html)
                    logger.debug(f'Extract Title: {searched_url}')
            logger.debug(f'Finished crawling')
            # TODO: May move all comparison techniques to similar class
            similars, fromm = self.similar.similar(url, title, content, searched_titles, searched_contents)
            if len(similars) > 0:
                top_similar = similars[0]
                return top_similar[0], {'type': fromm, 'value': top_similar[1]}
            return

        if title != '':
            if search_engine == 'google':
                search_results = search.google_search(f'{title}', site_spec_url=site)
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
                if len(search_results) >= 8:
                    search_results = search.google_search(f'"{title}"', use_db=self.use_db)
                    similar = search_once(search_results)
                    if similar is not None: 
                        return similar
            else:
                if site is not None:
                    site_str = f'site:{site}'
                else:
                    site_str = ''
                search_results = search.bing_search(f'{title} {site_str}', use_db=self.use_db)
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
                if len(search_results) >= 8:
                    search_results = search.bing_search(f'+"{title}"', use_db=self.use_db)
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