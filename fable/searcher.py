"""
Search broken pages' content
"""
import requests
from urllib.parse import urlparse 
from pymongo import MongoClient
import pymongo
import re, regex

from . import config, tools, tracer
from .utils import search, crawl, text_utils, url_utils

import logging
logging.setLoggerClass(tracer.tracer)
tracer = logging.getLogger('logger')
logging.setLoggerClass(logging.Logger)

he = url_utils.HostExtractor()
VERTICAL_BAR_SET = '\u007C\u00A6\u2016\uFF5C\u2225\u01C0\u01C1\u2223\u2502\u0964\u0965'

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
            tracer.wayback_url(url, wayback_url)
        except Exception as e:
            tracer.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
            wayback_url = url_utils.constr_wayback(url, '20201231')
            return
        tracer.title(url, title)
        search_results, searched = [], set()

        def search_once(search_results, typee):
            """Incremental Search"""
            global he
            nonlocal url, title, content, html, searched, search_engine
            searched_contents = {}
            searched_titles = {}
            search_cand = [s for s in search_results if s not in searched]
            tracer.search_results(url, search_engine, typee, search_results)
            searched.update(search_results)
            for searched_url in search_cand:
                searched_html = self.memo.crawl(searched_url, proxies=self.PS.select())
                if searched_html is None: continue
                searched_contents[searched_url] = self.memo.extract_content(searched_html)
                if he.extract(url) == he.extract(searched_url) or site == he.extract(searched_url):
                    searched_titles[searched_url] = self.memo.extract_title(searched_html)
            # TODO: May move all comparison techniques to similar class
            similars, fromm = self.similar.similar(wayback_url, title, content, searched_titles, searched_contents)
            if len(similars) > 0:
                top_similar = similars[0]
                return top_similar[0], {'type': fromm, 'value': top_similar[1]}
            return

        if title != '' and site:
            if search_engine == 'bing':
                site_str = f'site:{site}'
                bing_title = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} |\p{{Pd}}', title)
                bing_title = ' '.join(bing_title)
                print(bing_title)
                search_results = search.bing_search(f'{bing_title} {site_str}', use_db=self.use_db)
                if len(search_results) > 20: search_results = search_results[:20]
                similar = search_once(search_results, typee='title_site')
                if similar is not None: 
                    return similar
                if len(search_results) >= 8:
                    search_results = search.bing_search(f'+"{bing_title}" {site_str}', use_db=self.use_db)
                    if len(search_results) > 20: search_results = search_results[:20]
                    similar = search_once(search_results, typee='title_exact')
                    if similar is not None: 
                        return similar
            else:
                search_results = search.google_search(f'{title}', site_spec_url=site, use_db=self.use_db)
                similar = search_once(search_results, typee='title_site')
                if similar is not None: 
                    return similar
                if len(search_results) >= 8:
                    search_results = search.google_search(f'"{title}"', site_spec_url=site, use_db=self.use_db)
                    similar = search_once(search_results, typee='title_exact')
                    if similar is not None: 
                        return similar
        
        self.similar.tfidf._clear_workingset()
        topN = self.similar.tfidf.topN(content)
        topN = ' '.join(topN)
        tracer.topN(url, topN)
        if len(topN) > 0:
            if search_engine == 'bing':
                if site is not None:
                    site_str = f'site:{site}'
                else: 
                    site_str = ''
                search_results = search.bing_search(f'{topN} {site_str}', use_db=self.use_db)
                if len(search_results) > 20: search_results = search_results[:20]
                similar = search_once(search_results, typee='topN')
                if similar is not None: 
                    return similar
            else:
                search_results = search.google_search(topN, site_spec_url=site, use_db=self.use_db)
                similar = search_once(search_results, typee='topN')
                if similar is not None:
                    return similar
        return