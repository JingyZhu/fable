"""
Search broken pages' content
"""
import requests
from collections import defaultdict
from urllib.parse import urlparse 
from pymongo import MongoClient
import pymongo
import re
import tools

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

    def search(self, url):
        """
        Search by different queries type: {title_site, title_exact, topN_site}

        Returns: On no snapshot: None,
                 else: return {'query_type': {'google': [], 'bing': []}, 
                                'title': '', 'topN': '', 'site': site}
        """
        global he
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
        results_by_q = defaultdict(dict)
        results_by_q['title'] = title
        results_by_q['site'] = site
        if title != '':
            google_results = search.google_search(f'{title}', site_spec_url=site, use_db=self.use_db)
            if site is not None:
                site_str = f'site:{site}'
            else:
                site_str = ''
            bing_results = search.bing_search(f'{title} {site_str}', use_db=self.use_db)
            results_by_q['title_site'] = {'google': google_results, 'bing': bing_results}
            
            # Title exact
            google_results = search.google_search(f'"{title}"', use_db=self.use_db)
            bing_results = search.bing_search(f'+"{title}"', use_db=self.use_db)
            results_by_q['title_exact'] = {'google': google_results, 'bing': bing_results}
        else:
            results_by_q['title_site'] = {'google': [], 'bing': []}
            results_by_q['title_exact'] = {'google': [], 'bing': []}
        
        self.similar.tfidf._clear_workingset()
        topN = self.similar.tfidf.topN(content)
        topN = ' '.join(topN)
        results_by_q['topN'] = topN
        logger.info(f'topN: {topN}')
        if len(topN) > 0:
            google_results = search.google_search(topN, site_spec_url=site, use_db=self.use_db)  
            if site is not None:
                site_str = f'site:{site}'
            else: 
                site_str = ''
            bing_results = search.bing_search(f'{topN} {site_str}', use_db=self.use_db)
            results_by_q['topN_site'] = {'google': google_results, 'bing': bing_results}
        else:
            results_by_q['topN_site'] = {'google': [], 'bing': []}
        return results_by_q