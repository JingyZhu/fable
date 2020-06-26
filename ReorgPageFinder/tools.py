"""
Functions for determines whether two pages are simiar/same
Methodologies: Content Match / Parital Match
"""
import pymongo
from pymongo import MongoClient
import brotli
import re, os
import time
from collections import defaultdict
import random
import brotli
from dateutil import parser as dparser
from urllib.parse import urlsplit, urlparse

import sys
sys.path.append('../')
import config
from utils import text_utils, crawl, url_utils

import logging
logger = logging.getLogger('logger')

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder
DEFAULT_CACHE = 3600*24

def update_sites(collection):
    no_sites = list(collection.find({'site': {'$exists': False}}))
    he = url_utils.HostExtractor()
    for no_site in no_sites:
        site = he.extract(no_site['url'], wayback='web.archive.org' in no_site['url'])
        try:
            collection.update_one({'_id': no_site['_id']}, {'$set': {'site': site}})
        except: pass


class Memoizer:
    """
    Class for reducing crawl and wayback indexing
    """
    def __init__(self, use_db=True, db=db, proxies={}):
        """
        # TODO: Implement non-db version. (In mem version)
        """
        self.use_db = db
        if use_db:
            self.db = db
        self.PS = crawl.ProxySelector(proxies)
    
    def crawl(self, url, final_url=False, max_retry=0, **kwargs):
        """
        final_url: Whether also return final redirected URLS
        max_retry: Number of max retry times
        TODO: non-db version
        """
        if not final_url:
            html = self.db.crawl.find_one({'_id': url})
        else:
            html = self.db.crawl.find_one({'_id': url, 'final_url': {"$exists": True}})
        if html and html['ttl'] > time.time():
            if not final_url:
                return brotli.decompress(html['html']).decode()
            else:
                return brotli.decompress(html['html']).decode(), html['final_url']   
        retry = 0
        resp = crawl.requests_crawl(url, raw=True, **kwargs)
        if isinstance(resp, tuple) and resp[0] is None:
            logger.info(f'requests_crawl: Blocked url {url}, {resp[1]}')
            if not final_url:
                return None
            else:
                return None, None

        # Retry if get bad crawl
        while retry < max_retry and resp is None :
            retry += 1
            time.sleep(5)
            resp = crawl.requests_crawl(url, raw=True, **kwargs)
        if resp is None:
            logger.info(f'requests_crawl: Unable to get HTML of {url}')
            if not final_url:
                return None
            else:
                return None, None
        html = resp.text
        if final_url:
            fu = resp.url

        # Calculate cache expire date
        headers = {k.lower(): v.lower() for k, v in resp.headers.items()}
        cache_age = DEFAULT_CACHE
        if 'cache-control' in headers:
            v = headers['cache-control']
            pp_in = 'public' in v or 'private' in v
            maxage_in = 'max-age' in v
            v = v.split(',')
            if maxage_in:
                age = [int(vv.split('=')[1]) for vv in v if 'max-age' in vv][0]
                cache_age = max(cache_age, age)
            elif pp_in:
                cache_age = DEFAULT_CACHE*30
        ttl = time.time() + cache_age

        try:
            obj = {
                "_id": url,
                "url": url,
                "html": brotli.compress(html.encode()),
                "ttl": ttl
            }
            if final_url: obj.update({'final_url': fu})
            self.db.crawl.update_one({'_id': url}, {"$set": obj}, upsert=True)
        except Exception as e: logger.warn(f'crawl: {url} {str(e)}')
        if not final_url:
            return html
        else:
            return html, fu
    
    def wayback_index(self, url, **kwargs):
        """
        Get most representative and lastest snapshot for a certain url
        TODO: Non-db version
        """
        wayback_url = self.db.wayback_rep.find_one({"_id": url})
        if wayback_url:
            return wayback_url['wayback_url']
        param_dict = {
            "filter": ['statuscode:200', 'mimetype:text/html'],
            "collapse": "timestamp:8"
        }
        cps, status = crawl.wayback_index(url, param_dict=param_dict, total_link=True, **kwargs)
        if len(cps) == 0: # No snapshots
            logger.info(f"Wayback Index: No snapshots {status}")
            return
        cps.sort(key=lambda x: x[0])
        try:
            self.db.wayback_index.insert_one({
                "_id": url,
                'url': url,
                'ts': [c[0] for c in cps]
            })
        except: pass
        # Get latest 6 snapshots, and random sample 3 for finding representative results
        cps_sample = cps[-3:] if len(cps) >= 3 else cps
        cps_sample = [cp for cp in cps_sample if (dparser.parse(cps_sample[-1][0]) - dparser.parse(cp[0])).days <= 180]
        cps_dict = {}
        for ts, wayback_url, _ in cps_sample:
            html = self.crawl(wayback_url, proxies=self.PS.select())
            if html is None: continue
            # TODO: Domditiller vs Boilerpipe --> Acc vs Speed?
            content = text_utils.extract_body(html, version='boilerpipe')
            # title = text_utils.extract_title(html, version='newspaper')
            cps_dict[ts] = (ts, wayback_url, content)
        if len(cps_dict) > 0:
            rep = sorted(cps_dict.values(), key=lambda x: len(x[2].split()))[int((len(cps_dict)-1)/2)]
        else:
            rep = cps_sample[-1]
        try:
            self.db.wayback_rep.insert_one({
                "_id": url,
                "url": url,
                "ts": rep[0],
                "wayback_url": rep[1]
            })
        except Exception as e: pass
        return rep[1]
    
    def extract_content(self, html, **kwargs):
        if html is None:
            return ''
        html_bin = brotli.compress(html.encode())
        content = self.db.crawl.find_one({'html': html_bin, 'content': {"$exists": True}})
        if content:
            return content['content']
        content = text_utils.extract_body(html, **kwargs)
        try:
            self.db.crawl.update_one({'html': html_bin}, {"$set": {'content': content}})
        except Exception as e: logger.warn(f'extract content: {str(e)}')
        return content
    
    def extract_title(self, html, **kwargs):
        if html is None:
            return ''
        html_bin = brotli.compress(html.encode())
        title = self.db.crawl.find_one({'html': html_bin, 'title': {"$exists": True}})
        if title:
            return title['title']
        # Require to be extracted next time
        title = text_utils.extract_title(html, **kwargs)
        if title == "":
            return title
        try:
            self.db.crawl.update_one({'html': html_bin}, {"$set": {'title': title}})
        except Exception as e: logger.warn(f'extract title: {str(e)}')
        return title


class Similar:
    def __init__(self, use_db=True, db=db, corpus=[]):
        if not use_db and len(corpus) == 0:
            raise Exception("Corpus is requred for tfidf if db is not set")
        self.use_db = use_db
        self.threshold = 0.8
        self.short_threshold = self.threshold - 0.1
        if use_db:
            self.db =  db
            corpus = self.db.corpus.find({'$or': [{'src': 'realweb'}, {'usage': re.compile('represent')}]}, {'content': True})
            corpus = [c['content'] for c in list(corpus)] # TODO: Temp
            self.tfidf = text_utils.TFidfStatic(corpus)
        else:
            self.tfidf = text_utils.TFidfStatic(corpus)
            self.db = db # TODO: For testing only
        self.site = None
    
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
                if simi >= self.short_threshold:
                    return lws
            else:
                if wayback_sig[1] != anchor:
                    continue
                simi = 0
                for ws in wayback_sig[2]:
                    for ls in sig:
                        simi = max(simi, self.tfidf.simiar(ws, ls))
                if simi >= self.short_threshold:
                    return lws
        return None
    
    def content_similar(self, target_content, candidates_contents, candidates_html=None):
        """
        See whether there are content from candidates that is similar target
        candidates: {url: content}

        Return a list with all candidate higher than threshold
        """
        self.tfidf._clear_workingset()
        self.tfidf.add_corpus([target_content] + list(candidates_contents.values()))
        simi_cand = []
        for url, c in candidates_contents.items():
            simi = self.tfidf.similar(target_content, c)
            if simi >= self.threshold:
                simi_cand.append((url, simi))
        return sorted(simi_cand, key=lambda x: x[1], reverse=True)
    
    def _init_titles(self, site, version='domdistiller'):
        update_sites(self.db.crawl)
        if site == self.site:
            return
        logger.info(f'_init_titles {site}')
        self.site = site
        self.lw_titles = defaultdict(set)
        self.wb_titles = defaultdict(set)
        lw_crawl = list(self.db.crawl.find({'site': site, 'url': re.compile('^((?!web\.archive\.org).)*$')}))
        wb_crawl = list(self.db.crawl.find({'site': site, 'url': re.compile('web.archive.org')}))
        lw_crawl = [lw for lw in lw_crawl if 'title' in lw] + [lw for lw in lw_crawl if 'title' not in lw]
        wb_crawl = [wb for wb in wb_crawl if 'title' in wb] + [wb for wb in wb_crawl if 'title' not in wb]
        lw_path, wb_path = defaultdict(int), defaultdict(int)
        for lw in lw_crawl:
            loc_dir = (urlsplit(lw['url']).netloc.split(':')[0], os.path.dirname(urlsplit(lw['url']).path))
            if 'title' not in lw and lw_path[loc_dir] < 2:
                html = brotli.decompress(lw['html']).decode()
                title = text_utils.extract_title(html, version=version)
                if title == '': continue
                try:
                    self.db.crawl.update_one({'_id': lw['_id']}, {"$set": {'title': title}})
                except: pass
            elif 'title' in lw:
                title = lw['title']
            else: continue
            lw_path[loc_dir] += 1
            self.lw_titles[title].add(lw['url'])
        logger.info(f'lw_titles: {sum([len(v) for v in self.lw_titles.values()])}')
        seen = set()
        for wb in wb_crawl:
            wb_url = url_utils.filter_wayback(wb['url'])
            if wb_url in seen: continue
            else: seen.add(wb_url)
            loc_dir = (urlsplit(wb_url).netloc.split(':')[0], os.path.dirname(urlsplit(wb_url).path))
            if 'title' not in wb and wb_path[loc_dir] < 2:
                html = brotli.decompress(wb['html']).decode()
                title = text_utils.extract_title(html, version=version)
                if title == '': continue
                try:
                    self.db.crawl.update_one({'_id': wb['_id']}, {"$set": {'title': title}})
                except: pass
            elif 'title' in wb:
                title = wb['title']
            else: continue
            wb_path[loc_dir] += 1
            self.wb_titles[title].add(wb_url)
        logger.info(f'wb_titles: {sum([len(v) for v in self.wb_titles.values()])}')

    def title_similar(self, target_url, target_title, candidates_titles, fixed=True):
        """
        See whether there is UNIQUE title from candidates that is similar target
        candidates: {url: title}, with url in the same host!

        Return a list with all candidate higher than threshold
        """
        he = url_utils.HostExtractor()
        site = he.extract(target_url)
        if site != self.site:
            self._init_titles(site)
        if target_title in self.wb_titles:
            if len(self.wb_titles[target_title]) > 1:
                return []
            elif target_url not in self.wb_titles[target_title] and len(self.wb_titles[target_title]) > 0:
                return []
        self.tfidf._clear_workingset()
        self.tfidf.add_corpus([target_title] + list(candidates_titles.values()))
        simi_cand = []
        for url, c in candidates_titles.items():
            site = he.extract(url)
            if site != self.site and not fixed:
                self._init_titles(site)
            if c in self.lw_titles:
                if len(self.lw_titles[c]) > 1:
                    continue
                elif url not in self.lw_titles[c] and len(self.lw_titles[c]) > 0:
                    continue
            simi = self.tfidf.similar(target_title, c)
            if simi >= (self.short_threshold + self.threshold) / 2:
                simi_cand.append((url, simi))
        return sorted(simi_cand, key=lambda x: x[1], reverse=True)
    
    def clear_titles(self):
        self.site = None
        self.lw_titles = None
        self.wb_titles = None
    
    def similar(self, tg_url, tg_title, tg_content, cand_titles, cand_contents, cand_htmls=None, fixed=True):
        """
        All text-based similar tech is included
        Fixed: Whether title similarity is allowed across different sites
        """
        if self.site is not None:
            similars = self.title_similar(tg_url, tg_title, cand_titles, fixed=fixed)
            if len(similars) > 0:
                return similars
        similars = self.content_similar(tg_content, cand_contents, cand_htmls)
        return similars