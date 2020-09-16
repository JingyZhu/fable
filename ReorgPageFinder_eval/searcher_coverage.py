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

    def search_title_exact_site(self, url):
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
            if site is not None:
                site_str = f'site:{site}'
            else:
                site_str = ''
            
            # Title exact site
            google_results = search.google_search(f'"{title}"', site_spec_url=site, use_db=self.use_db)
            bing_results = search.bing_search(f'+"{title}" {site_str}', use_db=self.use_db)
            results_by_q['title_exact_site'] = {'google': google_results, 'bing': bing_results}
        else:
            results_by_q['title_exact_site'] = {'google': [], 'bing': []}
        
        return results_by_q
    
    def search_gt_10(self, url, search_engine='bing'):
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
                search_results = []
                for i in range(5):
                    search_results += search.google_search(f'{title}', site_spec_url=site, param_dict={'start': i*10})
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
                if len(search_results) >= 8:
                    search_results = []
                    for i in range(5):
                        search_results = search.google_search(f'"{title}"', param_dict={'start': i*10})
                    similar = search_once(search_results)
                    if similar is not None: 
                        return similar
            else:
                if site is not None:
                    site_str = f'site:{site}'
                else:
                    site_str = ''
                search_results = search.bing_search(f'{title} {site_str}', param_dict={'count': 50})
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
                if len(search_results) >= 8:
                    search_results = search.bing_search(f'+"{title}"', param_dict={'count': 50})
                    similar = search_once(search_results)
                    if similar is not None: 
                        return similar
        
        self.similar.tfidf._clear_workingset()
        topN = self.similar.tfidf.topN(content)
        topN = ' '.join(topN)
        logger.info(f'topN: {topN}')
        if len(topN) > 0:
            if search_engine == 'google':
                search_results = []
                for i in range(5):
                    search_results = search.google_search(topN, site_spec_url=site, param_dict={'start': i*10})
                similar = search_once(search_results)
                if similar is not None:
                    return similar
            else:
                if site is not None:
                    site_str = f'site:{site}'
                else: 
                    site_str = ''
                search_results = search.bing_search(f'{topN} {site_str}', param_dict={'count': 50})
                similar = search_once(search_results)
                if similar is not None: 
                    return similar
        return

    def similar_outlinks(self,  url):
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
        search_results, searched = [], set()
        similar_all = []

        def search_once(search_results):
            """Incremental Search"""
            global he
            nonlocal url, title, content, html, searched
            searched_contents = {}
            search_cand = [s for s in search_results if s not in searched]
            logger.info(f'#Search cands: {search_cand}')
            searched.update(search_results)
            for searched_url in search_cand:
                searched_html = self.memo.crawl(searched_url, proxies=self.PS.select())
                logger.debug(f'Crawl: {searched_url}')
                if searched_html is None: continue
                searched_contents[searched_url] = self.memo.extract_content(searched_html)
                logger.debug(f'Extract Content: {searched_url}')
            logger.debug(f'Finished crawling')
            # TODO: May move all comparison techniques to similar class
            similars = self.similar.content_similar(content, searched_contents, all_values=True)
            similars = [s for s in similars is he.extract(s[0]) not in [site, he.extract(url)]]
            return similars

        if title != '':
            search_results = search.google_search(f'{title}')
            similar_all += search_once(search_results)
            search_results = search.bing_search(f'{title}')
            similar_all += search_once(search_results)
        
        self.similar.tfidf._clear_workingset()
        topN = self.similar.tfidf.topN(content)
        topN = ' '.join(topN)
        
        logger.info(f'topN: {topN}')
        if len(topN) > 0:
            search_results = search.google_search(topN) 
            similar_all += search_once(search_results) 
            search_results = search.bing_search(f'{topN}')
            similar_all += search_once(search_results)
        similar_all.sort(key=lambda x: x[1], reverse=True)
        return similar_all
