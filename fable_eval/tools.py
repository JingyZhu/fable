"""
Functions for determines whether two pages are similar/same
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
from fable import config
from fable.utils import text_utils, crawl, url_utils, search

import logging
logger = logging.getLogger('logger')

db = config.DB
DEFAULT_CACHE = 3600*24
LEAST_SITE_URLS = 20 # Least # of urls a site must try to crawl to enable title comparison
COMMON_TITLE_SIZE = 5 # Common prefix/suffix extraction's sample number of title

he = url_utils.HostExtractor()

def update_sites(collection):
    global he
    no_sites = list(collection.find({'site': {'$exists': False}}))
    for no_site in no_sites:
        site = he.extract(no_site['url'], wayback='web.archive.org' in no_site['url'])
        try:
            collection.update_one({'_id': no_site['_id']}, {'$set': {'site': site}})
        except: pass


def title_common(titles):
    """Extract common parts of titles. Returns: set of common token"""
    if len(titles) == 0:
        return []
    common = set(re.split('_| \| |\|| - |-', titles[0]))
    for t in titles[1:]:
        common = common.intersection(re.split('_| \| |\|| - |-', t))
    return common


def unique_title(title, common):
    """Eliminate common suffix/prefix of certain site"""
    title_tokens = re.split('_| \| |\|| - |-', title)
    unique = []
    for token in title_tokens:
        if token not in common:
            unique.append(token)
    return ' '.join(unique)

def norm(url):
    us = urlsplit(url)
    if not us.query:
        return us.path
    else:
        return f"{us.path}?{us.query}"

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
        is_wayback = 'web.archive.org/web' in url
        if not final_url:
            html = self.db.crawl.find_one({'_id': url})
        else:
            html = self.db.crawl.find_one({'_id': url, 'final_url': {"$exists": True}})
        if html and (html['ttl'] > time.time() or is_wayback):
            if not final_url:
                return brotli.decompress(html['html']).decode()
            else:
                return brotli.decompress(html['html']).decode(), html['final_url']  
        elif html:
            try:
                self.db.crawl.update_one({'_id': url}, {'$unset': {'title': '', 'content': ''}}) 
            except: pass
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
                try:
                    age = [int(vv.split('=')[1]) for vv in v if 'max-age' in vv][0]
                    cache_age = max(cache_age, age)
                except:
                    cache_age = DEFAULT_CACHE
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
    
    def wayback_index(self, url, policy='latest-rep', ts=None, **kwargs):
        """
        Get most representative snapshot for a certain url
        policy: policy for getting which wayback snapshot
          - latest-rep: Lastest representitive
          - closest: Closest to ts (ts required)
          - closest-later: closest to ts but later (ts required)
          - closest-earlier: closest to ts but earlier (ts required)
          - earliest: earliest snapshot
          - latest: latest snapshot
          - all: all snapshots (return lists instead of str)
        TODO: Non-db version
        """
        assert(policy in {'latest-rep', 'closest-later', 'closest-earlier', 'earliest', 'latest', 'closest', 'all'})
        wayback_q = {"url": url, "policy": policy}
        if policy == 'latest-rep':
            wayback_url = self.db.wayback_rep.find_one(wayback_q)
            if wayback_url:
                return wayback_url['wayback_url']
        default_param = True
        default_key = {True: 'ts', False: 'ts_nb'}
        if 'param_dict' not in kwargs:
            param_dict = {
                "filter": ['statuscode:200', 'mimetype:text/html'],
                "collapse": "timestamp:8"
            }
        else:
            param_dict = kwargs['param_dict']
            del(kwargs['param_dict'])
            default_param = False
        cps = self.db.wayback_index.find_one({'url': url})
        if not cps or default_key[default_param] not in cps:
            cps, status = crawl.wayback_index(url, param_dict=param_dict, total_link=True, **kwargs)
            if len(cps) == 0: # No snapshots
                logger.info(f"Wayback Index: No snapshots {status}")
                return
            cps.sort(key=lambda x: x[0])
            try:
                self.db.wayback_index.update_one({"_id": url}, {'$set': {
                    'url': url,
                    default_key[default_param]: [c[0] for c in cps]
                }}, upsert=True)
            except: pass
        else:
            key = default_key[default_param]
            cps = [(c, url_utils.constr_wayback(url, c)) for c in cps[key]]

        if policy == 'closest':
            sec_diff = lambda x: (dparser.parse(str(x)) - dparser.parse(str(ts))).total_seconds()
            cps_close = [(cp, abs(sec_diff(cp[0]))) for cp in cps]
            return sorted(cps_close, key=lambda x: x[1])[0][0][1]
        elif policy == 'closest-later':
            cps_later = [cp for cp in cps if int(cp[0]) >= int(ts)]
            return cps_later[0][1] if len(cps_later) > 0 else cps[-1][1]
        elif policy == 'closest-earlier':
            cps_earlier = [cp for cp in cps if int(cp[0]) <= int(ts)]
            return cps_earlier[-1][1] if len(cps_earlier) > 0 else cps[0][1]
        elif policy == 'earliest':
            return cps[0][1]
        elif policy == 'latest':
            return cps[-1][1]
        elif policy == 'all':
            return cps
        elif policy == 'latest-rep':
            # Get latest 6 snapshots, and random sample 3 for finding representative results
            cps_sample = cps[-3:] if len(cps) >= 3 else cps
            cps_sample = [(cp[0], cp[1]) for cp in cps_sample if (dparser.parse(cps_sample[-1][0]) - dparser.parse(cp[0])).days <= 180]
            cps_dict = {}
            for ts, wayback_url in cps_sample:
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
                    "url": url,
                    "ts": rep[0],
                    "wayback_url": rep[1],
                    'policy': 'latest-rep'
                })
            except Exception as e: pass
            return rep[1]
        else:
            logger.error(f'Wayback Index: Reach non existed policy')
            raise
    
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
    def __init__(self, use_db=True, db=db, corpus=[], short_threshold=None):
        if not use_db and len(corpus) == 0:
            raise Exception("Corpus is requred for tfidf if db is not set")
        self.use_db = use_db
        self.threshold = 0.8
        self.short_threshold = short_threshold if short_threshold else self.threshold - 0.1
        if use_db:
            self.db =  db
            corpus = self.db.corpus.aggregate([
                {'$match':  {'$or': [{'src': 'realweb'}, {'usage': re.compile('represent')}]}},
                {'$project': {'content': True}},
                {'$sample': {'size': 100000}},
            ], allowDiskUse=True)
            corpus = [c['content'] for c in list(corpus)] # TODO: Temp
            # corpus = random.sample(corpus, 100000)
            self.tfidf = text_utils.TFidfStatic(corpus)
        else:
            self.tfidf = text_utils.TFidfStatic(corpus)
            self.db = db # TODO: For testing only
        self.site = None
    
    def match_url_sig(self, wayback_sig, liveweb_sigs):
        """
        See whether there is a url signature on liveweb that can match wayback sig
        Based on 2 methods: UNIQUE Similar anchor text, Non-UNIQUE same anchor text & similar sig
        
        Return: link_sig, similarity, by{anchor, sig}
        """
        self.tfidf._clear_workingset()
        anchor_count = defaultdict(set)
        corpus = [wayback_sig[1]] + [s for s in wayback_sig[2] if s != '']
        for link, anchor, sig in liveweb_sigs:
            anchor_count[anchor].add(link)
            corpus.append(anchor)
            for s in sig:
                if s != '': corpus.append(s)
        self.tfidf.add_corpus(corpus)
        for lws in liveweb_sigs:
            link, anchor, sig = lws
            if len(anchor_count[anchor]) < 2: # UNIQUE anchor
                simi = self.tfidf.similar(wayback_sig[1], anchor)
                if simi >= self.short_threshold:
                    return lws, simi, 'anchor'
            else:
                if wayback_sig[1] != anchor:
                    continue
                simi = 0
                for ws in wayback_sig[2]:
                    if ws == '': continue
                    for ls in sig:
                        if ls == '': continue
                        simi = max(simi, self.tfidf.similar(ws, ls))
                if simi >= self.short_threshold:
                    return lws, simi, 'sig'
        return None
    
    def max_similar(self, target_content, candidates_contents, init=True):
        """
        Return the max similarity between target_content and candidates_contents
        candidates_contents: List of strings
        init: Whether clear workingset and adding corpus is required. If not, must be pre-init

        Return: (similarity, content)
        """
        assert(isinstance(candidates_contents, list))
        max_simi, max_content = 0, None
        if init:
            self.tfidf._clear_workingset()
            self.tfidf.add_corpus([target_content] + candidates_contents)
        for c in candidates_contents:
            simi = self.tfidf.similar(target_content, c)
            if simi > max_simi:
                max_simi = simi
                max_content = c
        return max_simi, max_content

    def content_similar(self, target_content, candidates_contents, candidates_html=None, all_values=False):
        """
        See whether there are content from candidates that is similar target
        candidates: {url: content}
        all_values: Whether returns all values instead of matched ones

        Return a list with all candidate higher than threshold
        """
        self.tfidf._clear_workingset()
        self.tfidf.add_corpus([target_content] + list(candidates_contents.values()))
        simi_cand = []
        for url, c in candidates_contents.items():
            simi = self.tfidf.similar(target_content, c)
            logger.debug(f'simi: {simi}')
            if simi >= self.threshold or all_values:
                simi_cand.append((url, simi))
        return sorted(simi_cand, key=lambda x: x[1], reverse=True)
    
    def _init_titles(self, site, version='domdistiller'):
        update_sites(self.db.crawl)
        if site == self.site:
            return
        logger.info(f'_init_titles {site}')
        memo = Memoizer()
        self.site = site
        self.lw_titles = defaultdict(set)
        self.wb_titles = defaultdict(set)
        lw_crawl = list(self.db.crawl.find({'site': site, 'url': re.compile('^((?!web\.archive\.org).)*$')}))
        wb_crawl = list(self.db.crawl.find({'site': site, 'url': re.compile('web.archive.org')}))
        lw_crawl = random.sample(lw_crawl, 1000) if len(lw_crawl) > 1000 else lw_crawl
        wb_crawl = random.sample(wb_crawl, 500) if len(wb_crawl) > 500 else wb_crawl
        lw_crawl = [lw for lw in lw_crawl if 'title' in lw] + [lw for lw in lw_crawl if 'title' not in lw]
        wb_crawl = [wb for wb in wb_crawl if 'title' in wb] + [wb for wb in wb_crawl if 'title' not in wb]
        lw_path, wb_path = defaultdict(int), defaultdict(int)
        if len(lw_crawl) < LEAST_SITE_URLS:
            # Get more urls from search engine
            new_urls = search.bing_search(f"site:{site}", param_dict={'count': 50})
            iterr = 0
            in_lw = set([lw['url'] for lw in lw_crawl])
            while len(lw_crawl) < LEAST_SITE_URLS and iterr < len(new_urls):
                new_url = new_urls[iterr]
                iterr += 1
                if new_url in in_lw:
                    continue
                html = memo.crawl(new_url)
                if html is None:
                    continue
                in_lw.add(new_url)
                lw_crawl.append({'site': site, '_id': new_url, 'url': new_url, 'html': brotli.compress(html.encode())})

        for lw in lw_crawl:
            loc_dir = (urlsplit(lw['url']).netloc.split(':')[0], os.path.dirname(urlsplit(lw['url']).path))
            # Guarantee every path has at lease one title
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
            self.lw_titles[title].add(norm(lw['url'])) 
        self.lw_common = title_common(random.sample(self.lw_titles.keys(), min(COMMON_TITLE_SIZE, len(self.lw_titles.keys())) ))
        logger.info(f'lw_titles: {sum([len(v) for v in self.lw_titles.values()])} \n common: {self.lw_common}')
        seen = set()
        if len(wb_crawl) < LEAST_SITE_URLS:
            # Get more urls from wayback
            param_dict = {
                "filter": ['statuscode:200', 'mimetype:text/html'],
                "collapse": "urlkey",
                "limit": 300
            }
            new_urls, _ = crawl.wayback_index(f"*.{site}/*", param_dict=param_dict)
            iterr = 0
            in_wb = set([url_utils.filter_wayback(wb['url']) for wb in wb_crawl])
            new_urls = [n for n in new_urls if n[1] not in in_wb]
            new_urls = [url_utils.constr_wayback(n[1], n[0]) for n in random.sample(new_urls, min(LEAST_SITE_URLS, len(new_urls)))]
            while len(wb_crawl) < LEAST_SITE_URLS and iterr < len(new_urls):
                new_url = new_urls[iterr]
                iterr += 1
                if url_utils.filter_wayback(new_url) in in_wb:
                    continue
                html = memo.crawl(new_url)
                if html is None:
                    continue
                in_wb.add(new_url)
                wb_crawl.append({'site': site, '_id': new_url, 'url': new_url, 'html': brotli.compress(html.encode())})
        
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
            self.wb_titles[title].add(norm(wb_url))
        self.wb_common = title_common(random.sample(self.wb_titles.keys(), min(COMMON_TITLE_SIZE, len(self.wb_titles.keys())) ))
        logger.info(f'wb_titles: {sum([len(v) for v in self.wb_titles.values()])} \n common: {self.wb_common}')

    def title_similar(self, target_url, target_title, candidates_titles, fixed=True):
        """
        See whether there is UNIQUE title from candidates that is similar target
        candidates: {url: title}, with url in the same host!

        Return a list with all candidate higher than threshold
        """
        global he
        site = he.extract(target_url)
        if site != self.site:
            self._init_titles(site)
        if target_title in self.wb_titles:
            if len(self.wb_titles[target_title]) > 1:
                logger.debug(f'wayback title of url: {target_url} none UNIQUE')
                return []
            elif norm(target_url) not in self.wb_titles[target_title] and len(self.wb_titles[target_title]) > 0:
                logger.debug(f'wayback title of url: {target_url} none UNIQUE')
                return []
        else:
            self.wb_titles[target_title].add(target_url)
        self.tfidf._clear_workingset()
        self.tfidf.add_corpus([unique_title(target_title, self.wb_common)] + [unique_title(ct, self.lw_common) for ct in candidates_titles.values()])
        simi_cand = []
        for url, c in candidates_titles.items():
            site = he.extract(url)
            if site != self.site and not fixed:
                self._init_titles(site)
            if c in self.lw_titles:
                if len(self.lw_titles[c]) > 1:
                    logger.debug(f'title of url: {url} none UNIQUE')
                    continue
                elif norm(url) not in self.lw_titles[c] and len(self.lw_titles[c]) > 0:
                    logger.debug(f'title of url: {url} none UNIQUE')
                    continue
            simi = self.tfidf.similar(unique_title(target_title, self.wb_common), unique_title(c, self.lw_common))
            if simi >= (self.short_threshold + self.threshold) / 2:
                simi_cand.append((url, simi))
        return sorted(simi_cand, key=lambda x: x[1], reverse=True)
    
    def clear_titles(self):
        self.site = None
        self.lw_titles = None
        self.wb_titles = None
        self.lw_common = None
        self.wb_common = None
    
    def similar(self, tg_url, tg_title, tg_content, cand_titles, cand_contents, cand_htmls=None, fixed=True):
        """
        All text-based similar tech is included
        Fixed: Whether title similarity is allowed across different sites

        Return: [(similar urls, similarity)], from which comparison(title/content)
        """
        if self.site is not None:
            similars = self.title_similar(tg_url, tg_title, cand_titles, fixed=fixed)
            if len(similars) > 0:
                return similars, "title"
        similars = self.content_similar(tg_content, cand_contents, cand_htmls)
        return similars, "content"