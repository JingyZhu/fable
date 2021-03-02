"""
Functions for determines whether two pages are similar/same
Methodologies: Content Match / Parital Match
"""
import pymongo
from pymongo import MongoClient
import brotli
import re, os
import regex
import time
from collections import defaultdict
import random
import brotli
from dateutil import parser as dparser
from urllib.parse import urlsplit, urlparse
import bisect

from . import config, tracer
from .utils import text_utils, crawl, url_utils, search
from .utils.url_utils import url_norm
from .utils.sic_transit import text_norm

import logging
logging.setLoggerClass(tracer.tracer)
tracer = logging.getLogger('logger')
logging.setLoggerClass(logging.Logger)

db = config.DB
DEFAULT_CACHE = 3600*24
LEAST_SITE_URLS = 20 # Least # of urls a site must try to crawl to enable title comparison
COMMON_TITLE_SIZE = 5 # Common prefix/suffix extraction's sample number of title

VERTICAL_BAR_SET = '\u007C\u00A6\u2016\uFF5C\u2225\u01C0\u01C1\u2223\u2502\u0964\u0965'

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


def different_page(url, meta, content, crawls, wayback=False):
    """
    Return pages with differnt content in crawls, that has closest title to it
    meta: metadata to identify index, title if not wayback, ts otherwise
    wayback: Whether to consider ts
    """
    if not wayback:
        crawl_meta = [c['title'] for c in crawls]
    else:
        crawl_meta = [c['ts'] for c in crawls]
    left_idx = bisect.bisect(crawl_meta, meta)
    if left_idx >= len(crawls): left_idx -= 1
    right_idx = left_idx + 1
    left, right = left_idx >= 0, right_idx < len(crawls)
    while left or right:
        if left:
            crawl_left = crawls[left_idx]
            # TODO: Content comparison slows the process a lot
            if not url_utils.url_match(url, crawl_left['url']) \
              and text_utils.k_shingling(content, crawl_left.get('content', '')) < 0.9:
                return crawl_left 
            left_idx -= 1
            left = left_idx >= 0
        if right:
            crawl_right = crawls[right_idx]
            if not url_utils.url_match(url, crawl_right['url']) \
              and text_utils.k_shingling(content, crawl_right.get('content', '')) < 0.9:
                return crawl_right
            right_idx += 1
            right = right_idx < len(crawls)
    

def title_prepare(crawls, wayback=False):
    """
    Prepapre required data structures for unique_title
    crawls: URLs' crawls with title, HTML, content (if applicable)
    wayback: whether the common prefix/suffix extraction is for wayback urls. If set to False, mean liveweb pages.
    
    Returns: site_meta
        
    """
    netloc_dir = defaultdict(list)
    for ut in crawls:
        if 'content' not in ut:
            try:
                html = brotli.decompress(ut['html']).decode()
                ut['content'] = memo.extract_content(html, version='boilerpipe', handle_exception=False)
            except: pass
        if wayback:
            ut.update({
                'url': url_utils.filter_wayback(ut['url']),
                'ts': url_utils.get_ts(ut['url'])
            })
        nd = url_utils.netloc_dir(ut['url'])
        netloc_dir[nd].append(ut)
    url_meta = [[k, v] for k, v in netloc_dir.items()]
    url_meta.sort(key=lambda x: x[0])
    # * Sort the crawls in the same netloc_dir by title, so that same title are put together
    for i in range(len(url_meta)):
        if not wayback:
            url_meta[i][1] = sorted(url_meta[i][1], key=lambda x: x['title'])
        else:
            url_meta[i][1] = sorted(url_meta[i][1], key=lambda x: int(x['ts']))
    return url_meta


def token_intersect(title1_token, title2_token):
    """Intersection on two titles' token. Return intersections if available"""
    len1, len2 = len(title1_token), len(title2_token)
    len_m = len1 * len2
    if len_m > 1 and len_m in [len1, len2]: # * One don't have "-, |", the other have
        return set()
    elif len_m > 1: # *Both with "-, |"
        return set(title1_token).intersection(title2_token)
    else: # *Both without "-, |"
        return set()
        return set(title1_token[0].split()).intersection(title2_token[0].split())


def unique_title(url, title, content, site_url_meta, wayback=False, return_common_part=False):
    """
    Eliminate common suffix/prefix of certain site for liveweb
    url: full url (if wayback, url including web.archive.org)
    site_url_meta: [[netloc_dir, crawls]] sorted in netloc_dir
    
    Returns: prefix/suffix filtered title, prefix/suffix if return_common_part is True
    """
    title_tokens = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} |\p{{Pd}}', title)
    if wayback:
        url, ts = url_utils.filter_wayback(url), url_utils.get_ts(url)
    us = urlsplit(url)
    nd = url_utils.netloc_dir(url)
    close_idx = bisect.bisect_left(site_url_meta, [nd, []])
    if close_idx == len(site_url_meta):
        close_idx -= 1 # * In case close_idx is the out of bound
    upidx, downidx = close_idx, close_idx + 1
    diffs = set() # *{common_prefix_diff has seen, when -3 to 3 all seen, quit}
    itsts = set()
    striphost = lambda nd: nd[0].split(':')[0]

    # * Find first candidate which is not url itself
    upgoing = not (upidx < 0 \
                or striphost(site_url_meta[upidx][0]) != us.netloc.split(':')[0])
    downgoing = not (downidx > len(site_url_meta) - 1 \
                  or striphost(site_url_meta[downidx][0]) != us.netloc.split(':')[0])
    
    meta = ts if wayback else title
    while (upgoing or downgoing) and len(diffs) < 7:
        tocheck = []
        if upgoing:
            upcrawl = different_page(url, meta, content, site_url_meta[upidx][1], wayback=wayback)
            if upcrawl:
                upurl, uptitle = upcrawl['url'], upcrawl['title']
                upurl = url_utils.url_norm(upurl)
                uptd = url_utils.common_prefix_diff(url, upurl)
                if abs(uptd) <= 3 and not url_utils.url_match(upurl, url):
                    tocheck.append((uptd, upurl, uptitle))
        if downgoing:
            downcrawl = different_page(url, meta, content, site_url_meta[downidx][1], wayback=wayback)
            if downcrawl:
                downurl, downtitle = downcrawl['url'], downcrawl['title']
                downurl = url_utils.url_norm(downurl)
                downtd = url_utils.common_prefix_diff(url, downurl)
                if abs(downtd) <= 3 and not url_utils.url_match(downurl, url):
                    tocheck.append(((downtd, downurl, downtitle)))

        if len(tocheck) > 1: # *Put small in front
            tocheck[0], tocheck[1] = min(tocheck, key=lambda x:x[0]), max(tocheck, key=lambda x:x[0])
        for td, cand_url, cand_title in tocheck:
            tracer.debug(f'unique_title: compare with {cand_url} {cand_title}')
            diffs.add(td)
            itsts = token_intersect(title_tokens, \
                        regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} |\p{{Pd}}', cand_title))
            if len(itsts) > 0:
                break
        if len(itsts) > 0:
            break

        upidx -= 1
        downidx += 1
        upgoing = upgoing and not (upidx < 0 \
                        or striphost(site_url_meta[upidx][0]) != us.netloc.split(':')[0])
        downgoing = downgoing and not (downidx > len(site_url_meta) - 1 \
                        or striphost(site_url_meta[downidx][0]) != us.netloc.split(':')[0])
 
    if len(itsts) <= 0:
        return title
    if len(title_tokens) > 1:
        return ' '.join([tt for tt in title_tokens if tt not in itsts])
    else:
        return ' '.join([tt for tt in title_tokens[0].split() if tt not in itsts])


def norm_path(url):
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
            tracer.debug(f'memo.crawl: db has the valid crawl')
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
            tracer.info(f'requests_crawl: Blocked url {url}, {resp[1]}')
            if not final_url:
                return None
            else:
                return None, None

        # Retry if get bad crawl
        while retry < max_retry and resp is None:
            retry += 1
            time.sleep(5)
            resp = crawl.requests_crawl(url, raw=True, **kwargs)
        if resp is None:
            tracer.info(f'requests_crawl: Unable to get HTML of {url}')
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
                "site": he.extract(url, wayback=is_wayback),
                "html": brotli.compress(html.encode()),
                "ttl": ttl
            }
            if final_url: obj.update({'final_url': fu})
            self.db.crawl.update_one({'_id': url}, {"$set": obj}, upsert=True)
        except Exception as e: tracer.warn(f'crawl: {url} {str(e)}')
        tracer.debug(f'memo.crawl: upsert crawl {url}')
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
        cps = self.db.wayback_index.find_one({'_id': url})
        if not cps or default_key[default_param] not in cps:
            cps, status = crawl.wayback_index(url, param_dict=param_dict, total_link=True, **kwargs)
            tracer.debug('Wayback Index (tools.py): Get wayback query response')
            if len(cps) == 0: # No snapshots
                tracer.info(f"Wayback Index: No snapshots {status}")
                return
            cps.sort(key=lambda x: x[0])
            try:
                self.db.wayback_index.update_one({"_id": url}, {'$set': {
                    'url': url,
                    default_key[default_param]: [c[0] for c in cps]
                }}, upsert=True)
            except: pass
        else:
            tracer.debug('Wayback Index (tools.py): db has wayback_index')
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
            tracer.error(f'Wayback Index: Reach non existed policy')
            raise
    
    def extract_content(self, html, **kwargs):
        if html is None:
            if kwargs.get('handle_exception', True):
                return ''
            else:
                raise
        html_bin = brotli.compress(html.encode())
        try:
            content = self.db.crawl.find_one({'html': html_bin, 'content': {"$exists": True}})
        except: 
            content = None
        if content:
            return content['content']
        content = text_utils.extract_body(html, **kwargs)
        try:
            self.db.crawl.update_one({'html': html_bin}, {"$set": {'content': content}})
        except Exception as e: tracer.warn(f'extract content: {str(e)}')
        return content
    
    def extract_title(self, html, **kwargs):
        if html is None:
            if kwargs.get('handle_exception', True):
                return ''
            else:
                raise
        html_bin = brotli.compress(html.encode())
        try:
            title = self.db.crawl.find_one({'html': html_bin, 'title': {"$exists": True}})
        except: 
            title = None
        if title:
            return title['title']
        # Require to be extracted next time
        title = text_utils.extract_title(html, **kwargs)
        if title == "":
            return title
        try:
            self.db.crawl.update_one({'html': html_bin}, {"$set": {'title': title}})
        except Exception as e: tracer.warn(f'extract title: {str(e)}')
        return title
    
    def get_more_crawls(self, url, wayback=False):
        """
        Getting more samples from the same netloc_dir with url
        Process can look for different src:
         - First look at url's html's outgoing links
         - crawl for wayback (wb with close ts, lw with recent ts)
         - search for more urls

        url: full url
        Return: new crawls
        """
        tracer.debug(f'Getting more crawls from {url}')
        new_crawls = []
        seen_urls = set()
        # * Looking at outgoing links of url to see whether there are ones in the same  
        html = self.crawl(url)
        nd = url_utils.netloc_dir(url)
        if html:
            outlinks = crawl.outgoing_links(url, html, wayback=wayback)
            for outlink in outlinks:
                ond = url_utils.netloc_dir(outlink)
                if ond != nd or url_utils.url_match(outlink, url) or outlink in seen_urls:
                    continue
                try:
                    out_html = self.crawl(outlink)
                    out_title = self.extract_title(out_html, handle_exception=False)
                    out_content = self.extract_content(out_html, handle_exception=False)
                except: continue
                tracer.debug(f"get_more_crawls: Got new sample from outlinks {out_title} {out_html is None}")
                new_crawl = {
                    'url': outlink,
                    'html': out_html,
                    'title': out_title,
                    'content': out_content
                }
                new_crawls.append(new_crawl)
                seen_urls.add(outlink)
                # * Gather 2 data points should be sufficient
                if len(new_crawls) > 1:
                    return new_crawls
        
        url_prefix = ''.join(nd)
        if wayback:
            year = url_utils.get_ts(url)
            year = dparser.parse(year).year
        else:
            year = 2022
        param = {
            'from': year - 2,
            'to': year + 2,
            'filter': ['mimetype:text/html', 'statuscode:200'],
            'collapse': 'urlkey',
            'limit': 100
        }
        wayback_urls, _ = crawl.wayback_index(url_prefix, param_dict=param, total_link=True)
        wayback_urls = [wu[1] for wu in wayback_urls if url_utils.netloc_dir(wu[1]) == nd]
        if wayback:
            cand_urls = sorted(wayback_urls, key=lambda x: int(url_utils.get_ts(x)))
        else:
            cand_urls = [url_utils.filter_wayback(wu) for wu in wayback_urls]
        for cand_url in cand_urls:
            if cand_url in seen_urls or url_utils.url_match(url, cand_url):
                continue
            try:
                cand_html = self.crawl(cand_url)
                cand_title = self.extract_title(cand_html, handle_exception=False)
                cand_content = self.extract_content(cand_html, handle_exception=False)
            except: continue
            tracer.debug(f"get_more_crawls: Got new sample from wayback")
            new_crawl = {
                'url': cand_url,
                'html': cand_html,
                'title': cand_title,
                'content': cand_content
            }
            new_crawls.append(new_crawl)
            seen_urls.add(cand_url)
            # * Gather 2 data points should be sufficient
            if len(new_crawls) > 1:
                return new_crawls
        
        # TODO: Implement search if necessary
        return new_crawls


class Similar:
    def __init__(self, use_db=True, db=db, corpus=[], short_threshold=None, corpus_size=10000):
        """
        corpus_size: size of corpus to sample on: (0-250k)
        """
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
                {'$sample': {'size': corpus_size}},
            ], allowDiskUse=True)
            corpus = [c['content'] for c in list(corpus)]
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
        # TODO (new or not?): Not consider if the liveweb still have this link (only if exact match?)
        simis = []
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
                    simis.append((lws, simi, 'anchor'))
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
                    simis.append((lws, simi, 'sig'))
        if len(simis) == 0:
            return None
        else:
            return max(simis, key=lambda x: x[1])
    
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
            tracer.debug(f'simi: {simi}')
            if simi >= self.threshold:
                simi_cand.append((url, simi))
        return sorted(simi_cand, key=lambda x: x[1], reverse=True)
    
    def _init_titles(self, site, version='domdistiller'):
        """
        Return: Bool (whether init_title is succeed)
        """
        if self.site and site in self.site:
            return True
        memo = Memoizer()
        site_urls = [f'http://{site}', f'http://www.{site}']
        for site_url in site_urls:
            _, new_site = memo.crawl(site_url, final_url=True)
            if new_site:
                break
        if new_site is None:
            return False
        new_site = he.extract(new_site)
        self.site = (site, new_site)
        tracer.info(f'_init_titles {self.site}')
        # self.lw_titles = defaultdict(set) # *{title: set(path)}
        # self.wb_titles = defaultdict(set)
        self.lw_titles = defaultdict(list) # * {title: {list(crawls)}}
        self.wb_titles = defaultdict(list)
        lw_crawl = []
        for ssite in self.site:
            lw_crawl += list(self.db.crawl.find({'site': ssite, 'url': re.compile('^((?!web\.archive\.org).)*$')}))
        wb_crawl = list(self.db.crawl.find({'site': site, 'url': re.compile('\/\/web.archive.org')}))
        # lw_crawl = [lw for lw in lw_crawl if 'title' in lw] + [lw for lw in lw_crawl if 'title' not in lw]
        # wb_crawl = [wb for wb in wb_crawl if 'title' in wb] + [wb for wb in wb_crawl if 'title' not in wb]
        lw_crawl = [lw for lw in lw_crawl if 'title' in lw]
        wb_crawl = [wb for wb in wb_crawl if 'title' in wb]
        # lw_path, wb_path = defaultdict(int), defaultdict(int)
        
        self.lw_seen = set()
        start = time.time()
        # * Get more urls from search engine
        seen = set([lw['url'] for lw in lw_crawl])
        if len(lw_crawl) < LEAST_SITE_URLS:
            tracer.debug(f'_init_site: Not enough samples for liveweb page, fetching more')
            new_urls = search.bing_search(f"site:{self.site[-1]}", param_dict={'count': 50})
            iterr = 0
            while len(lw_crawl) < LEAST_SITE_URLS and iterr < len(new_urls):
                new_url = new_urls[iterr]
                iterr += 1
                if new_url in seen:
                    continue
                else:
                    seen.add(new_url)
                try:
                    html = memo.crawl(new_url)
                    content = memo.extract_content(html, handle_exception=False)
                    title = memo.extract_title(html, handle_exception=False)
                    lw_crawl.append({
                        'site': site, 
                        '_id': new_url, 
                        'url': new_url, 
                        'html': brotli.compress(html.encode()),
                        'title': title,
                        'content': content
                    })
                except:
                    continue

        # * Guarantee every path has at lease one title
        for lw in lw_crawl:
            url = url_norm(lw['url'])
            if url in self.lw_seen: continue
            else: self.lw_seen.add(url)
            loc_dir = url_utils.netloc_dir(lw['url'])
            # if 'title' not in lw and lw_path[loc_dir] < 2:
            #     html = brotli.decompress(lw['html']).decode()
            #     title = text_utils.extract_title(html, version=version)
            #     if title == '': continue
            #     try:
            #         self.db.crawl.update_one({'_id': lw['_id']}, {"$set": {'title': title}})
            #     except: pass
            # elif 'title' in lw:
            #     title = lw['title']
            # else: continue
            # lw_path[loc_dir] += 1
            # self.lw_titles[title].add(norm(lw['url']))
    
            lw.update({'netloc_dir': loc_dir})
            self.lw_titles[lw['title']].append(lw)
        # * Prepare data structures for title prefix/suffix filteration
        # lw_crawl_title = [lw for lw in lw_crawl if 'title' in lw]
        self.lw_meta = title_prepare(lw_crawl, wayback=False)
        end = time.time()
        tracer.info(f'lw_titles: {sum([len(v) for v in self.lw_titles.values()])}, init_time: {end-start:.2f}')

        start = time.time()
        self.wb_seen = set()
        for wb in wb_crawl:
            url, wb_url = wb['url'], url_norm(url_utils.filter_wayback(wb['url']))
            if wb_url in self.wb_seen: continue
            else: self.wb_seen.add(wb_url)
            loc_dir = url_utils.netloc_dir(wb_url)
            wb_title = wb.copy()
            wb_title.update({
                'url': wb_url, 
                'ts': url_utils.get_ts(url),
                'netloc_dir': loc_dir
            })
            self.wb_titles[wb_title['title']].append(wb_title)
        # * Prepare data structures for title prefix/suffix filteration
        self.wb_meta = title_prepare(wb_crawl, wayback=True)
        end = time.time()
        tracer.info(f'wb_titles: {sum([len(v) for v in self.wb_titles.values()])} \n init_time: {end - start:.2f}')
        return True

    def _add_crawl(self, url, title, content, html=None):
        """Add new crawls into similar comparison"""
        is_wayback = 'web.archive.org/web' in url
        if is_wayback and url_norm(url_utils.filter_wayback(url)) in self.wb_seen:
            return
        elif not is_wayback and url_norm(url) in self.lw_seen:
            return
        elif not title:
            return
        if is_wayback:
            ts, url = url_utils.get_ts(url), url_utils.filter_wayback(url)
        nd = url_utils.netloc_dir(url)
        toadd = {
            'url': url,
            'html': html,
            'title': title,
            'content': content,
            'netloc_dir': nd
        }
        if is_wayback:
            toadd.update({'ts': ts})
            self.wb_titles[title].append(toadd)
            nd_idx = bisect.bisect_left(self.wb_meta, [nd, []])
            if nd_idx >= len(self.wb_meta) or self.wb_meta[nd_idx][0] != nd:
                self.wb_meta.insert(nd_idx, [nd, [toadd]])
            else:
                tss = [int(obj['ts']) for obj in self.wb_meta[nd_idx][1]]
                ts_idx = bisect.bisect_right(tss, int(ts))
                self.wb_meta[nd_idx][1].insert(ts_idx, toadd)
            self.wb_seen.add(url_norm(url))
        else:
            self.lw_titles[title].append(toadd)
            nd_idx = bisect.bisect_left(self.lw_meta, [nd, []])
            if nd_idx >= len(self.lw_meta) or self.lw_meta[nd_idx][0] != nd:
                self.lw_meta.insert(nd_idx, [nd, [toadd]])
            else:
                titles = [obj['title'] for obj in self.lw_meta[nd_idx][1]]
                title_idx = bisect.bisect_right(titles, toadd['title'])
                self.lw_meta[nd_idx][1].insert(title_idx, toadd)
            self.lw_seen.add(url_norm(url))

    def shorttext_match(self, text1, text2):
        """
        Func should only be called when self.tfidf is properly prepared
        Check whether one text is a subset of another + similar enough
        # TODO: Currently use TF-IDF for comparison, but other way may also apply

        Returns: 0 if not match. Actual similarity otherwise
        """
        text1_token, text2_token = text_utils.tokenize(text1), text_utils.tokenize(text2)
        text1_token, text2_token = ' '.join(text1_token), ' '.join(text2_token)
        # * To match, one text must be the subset of another
        if text1_token not in text2_token and text2_token not in text1_token:
            tracer.debug(f'shorttext_match: one text not a subset of another: \n{ text1} vs. {text2} \n {text1_token} vs. {text2_token}')
            return 0
        tracer.debug(f'shorttext_match: simi between {text1} \n {text2}')
        simi = self.tfidf.similar(text1, text2)
        if simi >= (self.short_threshold + self.threshold) / 2:
            return simi
        else:
            return 0

    def _is_title_unique(self, url, title, content, wayback=False):
        """
        Check is input title is unique among the sites
        If there is no sample in the netloc_dir, use memo to get for more
        Also insert more samples into lw_meta/wb_meta

        Return: Bool
        """
        if not title:
            return False
        lw_url = url_utils.filter_wayback(url)
        site_titles = self.wb_titles if wayback else self.lw_titles
        site_meta = self.wb_meta if wayback else self.lw_meta
        nd = url_utils.netloc_dir(lw_url)
        def check_titles():
            if title in site_titles:
                for site_crawl in site_titles[title]:
                    # * title in site_titles is a child of url
                    if nd != site_crawl['netloc_dir'] and nd in site_crawl['netloc_dir']:
                        continue
                    # TODO: Can actually also compare content to not consider canonical here
                    if not url_utils.url_match(lw_url, site_crawl['url']) and \
                            text_utils.k_shingling(content, site_crawl.get('content', '')) < 0.9:
                        tracer.debug(f"_is_title_unique: title {title} is not unique amoung site with {site_crawl['url']}")
                        return False
            return True
        unique = check_titles()
        if not unique: return unique
        nd_idx = bisect.bisect_left(site_meta, [nd, []])
        # * Get more samples if nd's URL is not enough
        if wayback and len(site_meta[nd_idx][1]) < 2:
            memo = Memoizer()
            more_crawls = memo.get_more_crawls(url, wayback=True)
            for more_crawl in more_crawls:
                self._add_crawl(more_crawl['url'], more_crawl['title'], more_crawl['content'], more_crawl['html'],)
            return check_titles()
        return True
        

    def title_similar(self, target_url, target_title, target_content, candidates_titles, candidates_contents, fixed=True):
        """
        See whether there is UNIQUE title from candidates that is similar target
        target_url: URL in the wayback form
        candidates_x: {url: x}, with url in the same host!

        Return a list with all candidate higher than threshold
        """
        global he
        site = he.extract(target_url, wayback=True)
        if site not in self.site:
            self._init_titles(site)
        # if target_title in self.wb_titles:
        #     if len(self.wb_titles[target_title]) > 1:
        #         tracer.debug(f'wayback title of url: {lw_target_url} us not UNIQUE: {self.wb_titles[target_title]}')
        #         return []
        #     elif norm(lw_target_url) not in self.wb_titles[target_title] and len(self.wb_titles[target_title]) > 0:
        #         tracer.debug(f'wayback title of url: {lw_target_url} is not UNIQUE: {self.wb_titles[target_title]}')
        #         return []
        # else:
        #     self.wb_titles[target_title].add(lw_target_url)
        if not self._is_title_unique(target_url, target_title, target_content, wayback=True):
            tracer.debug(f"title_similar: target_url's title '{target_title}' is not unique")
            return []

        self.tfidf._clear_workingset()
        # print([[v[0], [vv['url'] for vv in v[1]] ] for v in self.wb_meta])
        # print({k: [vv['url'] for vv in v] for k, v in self.wb_titles.items()})
        # * Extract Unique Titles for both wb urls and lw urls
        tgt_uniq_title = unique_title(target_url, target_title, target_content, self.wb_meta, wayback=True)
        cand_uniq_titles = {url: unique_title(url, title, candidates_contents.get(url, ''), self.lw_meta, wayback=False) \
             for url, title in candidates_titles.items()}
        self.tfidf.add_corpus([tgt_uniq_title] + [ct for ct in cand_uniq_titles.values()])

        simi_cand = []
        for url in cand_uniq_titles:
            c, uniq_c = candidates_titles[url], cand_uniq_titles[url]
            site = he.extract(url)
            if site not in self.site and not fixed:
                self._init_titles(site)
            # if c in self.lw_titles:
            #     if len(self.lw_titles[c]) > 1:
            #         tracer.debug(f'title of url: {url} none UNIQUE: {self.lw_titles[c]}')
            #         continue
            #     elif norm(url) not in self.lw_titles[c] and len(self.lw_titles[c]) > 0:
            #         tracer.debug(f'title of url: {url} none UNIQUE: {self.lw_titles[c]}')
            #         continue
            if not self._is_title_unique(url, c, candidates_contents.get(url, ''), wayback=False):
                tracer.debug(f"title_similar: cand_url's title '{c}' is not unique")
                continue
            simi = self.shorttext_match(tgt_uniq_title, uniq_c)
            if simi:
                simi_cand.append((url, simi))

        return sorted(simi_cand, key=lambda x: x[1], reverse=True)
    
    def clear_titles(self):
        self.site = None
        self.lw_titles = None
        self.wb_titles = None
        # self.lw_index = None
        self.lw_meta = None
        # self.wb_index = None
        self.wb_meta = None
        self.lw_seen = None
        self.wb_seen = None
    
    def similar(self, tg_url, tg_title, tg_content, cand_titles, cand_contents, \
                cand_htmls={}, fixed=True):
        """
        All text-based similar tech is included
        Fixed: Whether title similarity is allowed across different sites

        Return: [(similar urls, similarity)], from which comparison(title/content)
        """
        # tg_url, tg_title, tg_content, tg_html = tg_crawl['url'], tg_crawl['title'], tg_crawl['content'], tg_crawl['html']
        # cand_titles = {c['url']: c['title'] for c in cand_crawls if 'title' in c}
        # cand_contents = {c['url']: c['content'] for c in cand_crawls if 'content' in c}
        # cand_htmls = {c['url']: c['html'] for c in cand_crawls if 'html' in c}
        
        self._add_crawl(tg_url, tg_title, tg_content)
        for cand_url in cand_titles:
            self._add_crawl(cand_url, cand_titles[cand_url], cand_contents.get(cand_url), \
                            cand_htmls.get(cand_url))
        if self.site is not None and tg_title:
            similars = self.title_similar(tg_url, tg_title, tg_content, cand_titles, cand_contents, fixed=fixed)
            if len(similars) > 0:
                return similars, "title"
        similars = self.content_similar(tg_content, cand_contents, cand_htmls)
        return similars, "content"


def is_canonical(url1, url2, resp1=None, resp2=None, use_resp=False):
    """
    Decide whether two URLs are canonical of each other
    Both url1 and url2 should be good pages
    Check with metrics:
    1. URLs' final urls are the same
    2. URLs' response canonical are the same
    3. Responses are indifferentiable

    Return: (currently) boolean on if is canonical
    """
    if url_match(url1, url2):
        return True
    if not use_resp:
        resp1 = crawl.requests_crawl(url1, raw=True)
        resp2 = crawl.requests_crawl(url2, raw=True)
        if isinstance(resp1, tuple) or not resp1:
            return False
        if isinstance(resp2, tuple) or not resp2:
            return False
    # * Check final url
    if url_match(resp1.url, resp2.url):
        return True
    # * Check canonical
    try:
        soup1 = BeautifulSoup(resp1.text, 'lxml')
        soup2 = BeautifulSoup(resp2.text, 'lxml')
        cans1 = soup1.find_all('link', {'rel': 'canonical'})
        cans2 = soup2.find_all('link', {'rel': 'canonical'})
        can1, can2 = '', ''
        if len(cans1) > 0:
            if urlsplit(resp1.url).path not in ['', '/']:
                can1 = cans1[0]['href']
        if len(cans1) > 0:
            if urlsplit(resp2.url).path not in ['', '/']:
                can2 = cans2[0]['href']
        # * Check for potential match
        if can1 and can2 and url_match(can1, can2):
            return True
        if can1 and url_match(can1, resp2.url):
            return True
        if can2 and url_match(can2, resp1.url):
            return True
    except:
        pass
    try:
        content1 = soup1.get_text(separator=' ')
    except:
        content1 = resp1.text
    try:
        content2 = soup2.get_text(separator=' ')
    except:
        content2 = resp2.text
    if text_utils.k_shingling(text_norm(content1), text_norm(content2)) >= 0.95:
        return True
    return False