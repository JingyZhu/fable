"""
Stawman approach for Searching broken pages' content
Advanced version: Looking at the first snapshot
"""
import requests
from urllib.parse import urlparse 
from pymongo import MongoClient
import pymongo
import re
from . import tools
from collections import defaultdict

import sys
sys.path.append('../')
from fable import config
from fable.utils import search, crawl, text_utils, url_utils, sic_transit

import logging
logger = logging.getLogger('logger')

db = config.DB
he = url_utils.HostExtractor()

class StrawmanSearcher:
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
            wayback_url = self.memo.wayback_index(url, policy='earliest')
            html = self.memo.crawl(wayback_url)
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
                searched_html = self.memo.crawl(searched_url)
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


class StrawmanFinder:
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, logger=None, logname=None):
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar(short_threshold=0.75)
        self.PS = crawl.ProxySelector(proxies)
        self.searcher = StrawmanSearcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
        self.pattern_dict = None
        self.logname = './StrawmanFinder_adv.log' if logname is None else logname
        self.logger = logger if logger is not None else self._init_logger()

    def _init_logger(self):
        logger = logging.getLogger('logger')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s %(asctime)s [%(filename)s %(funcName)s:%(lineno)s]: \n %(message)s')
        file_handler = logging.FileHandler(self.logname)
        file_handler.setFormatter(formatter)
        std_handler = logging.StreamHandler()
        std_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(std_handler)
        return logger
    
    def init_site(self, site, urls):
        self.site = site
        # already_in = list(self.db.reorg.find({'hostname': site}))
        # already_in = set([u['url'] for u in already_in])
        # for url in urls:
            # if url in already_in:
            #     continue
            # if not sic_transit.broken(url):
            #     try:
            #         self.db.na_urls.update_one({'_id': url}, {'$set': 
            #             {'url': url, 'hostname': site, 'url': url, 'false_positive': True}}, upsert=True)
            #     except: pass
            # try:
            #     self.db.reorg.update_one({'url': url}, {'$set': {
            #         'url': url,
            #         'hostname': site
            #     }}, upsert=True)
            # except: pass
        # reorg_urls = self.db.reorg.find({'hostname': site, 'reorg_url': {"$exists": True}})
        # for reorg_url in list(reorg_urls):
        #     # Patch the no title urls
        #     if 'title' not in reorg_url:
        #         wayback_reorg_url = self.memo.wayback_index(reorg_url['url'])
        #         reorg_html, wayback_reorg_url = self.memo.crawl(wayback_reorg_url, final_url=True)
        #         reorg_title = self.memo.extract_title(reorg_html, version='domdistiller')
        #         reorg_url['title'] = reorg_title
        #         self.db.reorg.update_one({'url': reorg_url['url']}, {'$set': {'title': reorg_title}})
        if len(self.logger.handlers) > 2:
            self.logger.handlers.pop()
        formatter = logging.Formatter('%(levelname)s %(asctime)s [%(filename)s %(funcName)s:%(lineno)s]: \n %(message)s')
        file_handler = logging.FileHandler(f'./logs/{site}.strawadv.log')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def clear_site(self):
        self.site = None
        self.logger.handlers.pop()

    def fp_check(self, url, reorg_url):
        """
        Determine False Positive

        returns: Boolean on if false positive
        """
        if url_utils.url_match(url, reorg_url):
            return True
        html, url = self.memo.crawl(url, final_url=True)
        reorg_html, reorg_url = self.memo.crawl(reorg_url, final_url=True)
        if html is None or reorg_html is None:
            return False
        content = self.memo.extract_content(html)
        reorg_content = self.memo.extract_content(reorg_html)
        self.similar.tfidf._clear_workingset()
        simi = self.similar.tfidf.similar(content, reorg_content)
        return simi >= 0.8

    def search(self, required_urls=None):
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        # _search
        noreorg_urls = list(self.db.reorg.find({"hostname": self.site, 'reorg_url_strawman_adv': {"$exists": False}}))
        searched_checked = self.db.checked.find({"hostname": self.site, "strawman_adv": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        
        required_urls = set(required_urls) if required_urls else set([u['url'] for u in noreorg_urls])

        urls = [u for u in noreorg_urls if u['url'] not in searched_checked and u['url'] in required_urls]
        broken_urls = set([u['url'] for u in urls]) # Filter out urls not in sample
        self.logger.info(f'Straw SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            searched = self.searcher.search(url, search_engine='bing')
            if searched is None:
                searched = self.searcher.search(url, search_engine='google')
            update_dict = {}
            has_title = self.db.reorg.find_one({'url': url})
            # if has_title is None: # No longer in reorg (already deleted)
            #     continue
            if 'title' not in has_title or has_title['title'] == 'N/A':
                try:
                    wayback_url = self.memo.wayback_index(url, policy='latest-rep')
                    html = self.memo.crawl(wayback_url)
                    title = self.memo.extract_title(html, version='domdistiller')
                except: # No snapthost on wayback
                    self.logger.error(f'WB_Error {url}: Fail to get data from wayback')
                    try: self.db.na_urls.update_one({'_id': url}, {"$set": {
                        'url': url,
                        'hostname': self.site,
                        'no_snapshot': True
                    }}, upsert=True)
                    except: pass
                    continue
                update_dict = {'title': title}
            else:
                title = has_title['title']


            if searched is not None:
                searched, trace = searched
                self.logger.info(f"HIT_Straw_adv: {searched}")
                fp = self.fp_check(url, searched)
                if not fp: # False positive test
                    # _search
                    update_dict.update({'reorg_url_strawman_adv': searched, 'by_strawman_adv':{
                        "method": "search"
                    }})
                    update_dict['by_strawman_adv'].update(trace)
                else:
                    try: self.db.na_urls.update_one({'_id': url}, {'$set': {
                            'url': url,
                            'false_positive_strawman_adv': True, 
                            'hostname': self.site
                        }}, upsert=True)
                    except: pass
                    searched = None


            if len(update_dict) > 0:
                try:
                    self.db.reorg.update_one({'url': url}, {"$set": update_dict}) 
                except Exception as e:
                    self.logger.warn(f'Strawman search update DB: {str(e)}')
            searched_checked.add(url)
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    "strawman_adv": True
                }}, upsert=True)
            except: pass