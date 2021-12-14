"""
Check for wayback alias
"""
import os
from urllib.parse import urlsplit, urlparse, parse_qsl, parse_qs, urlunsplit
from bs4 import BeautifulSoup
from queue import Queue
from collections import defaultdict
import re, json
from dateutil import parser as dparser
import datetime

from . import config, tools, tracer
from .utils import crawl, url_utils, sic_transit

import logging
logging.setLoggerClass(tracer.tracer)
tracer = logging.getLogger('logger')
logging.setLoggerClass(logging.Logger)

he = url_utils.HostExtractor()

def _safe_dparse(ts):
    try:
        return dparser.parse(ts)
    except:
        return datetime.datetime.now()

class HistRedirector:
    def __init__(self, corpus=[], proxies={}, memo=None):
        self.corpus = corpus
        self.PS = crawl.ProxySelector(proxies)
        self.memo = memo if memo is not None else tools.Memoizer()
    
    def _verify_alias(self, url, new_urls, ts, homepage_redir, strict_filter, require_neighbor, seen_redir_url):
        """
        Verify whether new_url is valid alias by checking:
            1. new_urls is in the same site
            2. new_urls is working 
            3. whether there is no other url in the same form redirected to this url
        """
        global tracer
        new_url = new_urls[-1]

        # * If new url is in the same site
        orig_host = he.extract(url)
        host_url = f'http://{orig_host}'
        new_host = he.extract(new_url)
        new_host_url = f'http://{new_host}'
        _, orig_host = self.memo.crawl(host_url, final_url=True)
        _, new_host = self.memo.crawl(new_host_url, final_url=True)
        if orig_host is None or new_host is None or he.extract(new_host) != he.extract(orig_host):
            tracer.debug('verify_alias: redirected URL not in the same site')
            return False

        # *If homepage to homepage redir, no soft-404 will be checked
        broken, _ = sic_transit.broken(new_url, html=True, ignore_soft_404_content=homepage_redir)
        if broken: return False
        if homepage_redir: return True
        if isinstance(ts, str): ts = dparser.parse(ts)
        ts_year = ts.year

        # * Perform strict filter if set to true
        if strict_filter:
            new_us = urlsplit(new_url)
            us = urlsplit(url)
            if not new_us.query and not us.query and new_us.path in us.path:
                return False
        
        # *If url ended with / (say /dir/), consider both /* and /dir/*
        url_prefix = urlsplit(url)
        url_dir = [url_utils.nondigit_dirname(url_prefix.path)]
        # // if url_prefix.path[-1] == '/': url_dir.append(os.path.dirname(url_dir[0]))
        url_prefix = url_prefix._replace(path=url_dir[-1] + '/*', query='')
        url_prefix = urlunsplit(url_prefix)
        param_dict = {
            'from': str(ts_year) + '0101',
            'to': str(ts_year) + '1231',
            "filter": ['statuscode:3[0-9]*', 'mimetype:text/html'],
            'limit': 100
        }
        tracer.debug(f'Search for neighbors with query & year: {url_prefix} {ts_year}')
        neighbor, _ = crawl.wayback_index(url_prefix, param_dict=param_dict, total_link=True)

        # *Get closest crawled urls in the same dir, which is not target itself  
        not_match = lambda u: not url_utils.url_match(url, url_utils.filter_wayback(u) ) # and not url_utils.filter_wayback(u)[-1] == '/'
        same_netdir = lambda u: url_utils.nondigit_dirname(urlsplit(url_utils.filter_wayback(u)).path[:-1]) in url_dir
        _path_length = lambda url: len(list(filter(lambda x: x != '', urlsplit(url).path.split('/'))))
        same_length = lambda u: _path_length(url) == _path_length(url_utils.filter_wayback(u))

        neighbor = sorted([n for n in neighbor if not_match(n[1]) \
                                                and same_netdir(n[1]) \
                                                and same_length(n[1])], \
                            key=lambda x: abs((_safe_dparse(x[0]) - ts).total_seconds()))
        tracer.debug(f'neightbor: {len(neighbor)}')
        matches = []
        for i in range(min(5, len(neighbor))):
            try:
                tracer.debug(f'Choose closest neighbor: {neighbor[i][1]}')
                response = crawl.requests_crawl(neighbor[i][1], raw=True)
                neighbor_urls = [r.url for r in response.history[1:]] + [response.url]
                match = False
                for neighbor_url in neighbor_urls:
                    for new_url in new_urls:    
                        thismatch = url_utils.url_match(new_url, neighbor_url)
                        if thismatch: 
                            match = True
                            seen_redir_url.add(new_url)
                matches.append(match)
                if i > 0: # * Chech for two neighbors
                    break
            except Exception as e:
                tracer.debug(f'Cannot check neighbor on wayback_alias')
                continue
        if require_neighbor and len(matches) == 0:
            tracer.debug(f'require_neighbor is set to True, but there are no neighbors that can be checked')
            return False
        if True in matches:
            tracer.debug(f'url in same dir: {neighbor[0][1]} redirects to the same url')
            return False
        return True

    def wayback_alias(self, url, require_neighbor=False, homepage_redir=True, strict_filter=False):
        """
        Utilize wayback's archived redirections to find the alias/reorg of the page
        Not consider non-homepage to homepage
        If latest redirection is invalid, iterate towards earlier ones (separate by every month)
        require_neighbor: Whether a redirection neighbor is required to do the comparison
        homepage_redir: Whether redirection to homepage (from non-homepage) is considered valid
        strict_filter: Not consider case where: redirected URL's path is a substring of the original one

        Returns: reorg_url is latest archive is an redirection to working page, else None
        """
        tracer.debug('Start wayback_alias')
        us = urlsplit(url)
        is_homepage = us.path in ['/', ''] and not us.query
        try:
            wayback_ts_urls = self.memo.wayback_index(url, policy='all', all_none_400=True)
        except: return

        if not wayback_ts_urls or len(wayback_ts_urls) == 0:
            return

        wayback_ts_urls = [(_safe_dparse(c[0]), c[1]) for c in wayback_ts_urls]

        # * Check for 400 snapshots, any redirections after it will not be counted
        param_dict = {
            'url': url,
            'filter': ['mimetype:text/html', 'statuscode:[4][0-9]*'],
            'output': 'json'
        }
        broken_archives, _ = crawl.wayback_index(url, param_dict=param_dict)
        if len(broken_archives):
            broken_ts = _safe_dparse(broken_archives[0][0])
            wayback_ts_urls = [w for w in wayback_ts_urls if w[0] < broken_ts]
            it = len(wayback_ts_urls) - 1
        
        # * Count for unmatched wayback final url, and wayback_alias to same redirected fake alias
        url_match_count, same_redir = 0, 0
        it = len(wayback_ts_urls) - 1
        last_ts = wayback_ts_urls[-1][0] + datetime.timedelta(days=90)
        seen_redir_url = set()
        while url_match_count < 3 and same_redir < 5 and it >= 0:
            ts, wayback_url = wayback_ts_urls[it]
            tracer.debug(f'wayback_alias iteration: ts: {ts} it: {it}')
            it -= 1
            if ts + datetime.timedelta(days=90) > last_ts: # 2 snapshots too close
                continue
            try:
                response = crawl.requests_crawl(wayback_url, raw=True)
                wayback_url = response.url
                match = url_utils.url_match(url, url_utils.filter_wayback(wayback_url))
            except:
                continue

            # *Not match means redirections, the page could have a temporary redirections to the new page
            if not match:
                last_ts = ts
                new_url = url_utils.filter_wayback(wayback_url)
                inter_urls = [url_utils.filter_wayback(wu.url) for wu in response.history] # Check for multiple redirections
                inter_urls.append(new_url)
                inredir = False
                for inter_url in inter_urls[1:]:
                    if inter_url in seen_redir_url:
                        inredir = True
                if inredir:
                    same_redir += 1
                    continue
                else:
                    seen_redir_url.add(new_url)
                inter_uss = [urlsplit(inter_url) for inter_url in inter_urls]
                tracer.info(f'Wayback_alias: {ts}, {inter_urls}')

                # *If non-home URL is redirected to homepage, it should not be a valid redirection
                new_is_homepage = True in [inter_us.path in ['/', ''] and not inter_us.query for inter_us in inter_uss]
                if not homepage_redir and new_is_homepage and (not is_homepage): 
                   continue
                
                live_new_url = inter_urls[-1]
                live_new_url = self.na_alias(live_new_url)
                if live_new_url is None:
                    continue
                # //pass_check, reason = sic_transit.broken(new_url, html=True, ignore_soft_404=is_homepage and new_is_homepage)
                # //ass_check = not pass_check
                if len(inter_urls) > 1:
                    inter_urls = inter_urls[1:]
                pass_check = self._verify_alias(url, inter_urls, ts, homepage_redir=is_homepage and new_is_homepage, \
                                                strict_filter=strict_filter, require_neighbor=require_neighbor, seen_redir_url=seen_redir_url)
                if pass_check:
                    return live_new_url
            else:
                url_match_count += 1
        return

    def na_alias(self, alias):
        """Check whether found alias are N/A"""
        # * If today's url is not in the same site, not a valid redirection
        new_host = he.extract(alias)
        new_host_url = f'http://{new_host}'
        _, new_host = self.memo.crawl(new_host_url, final_url=True)
        _, alias = self.memo.crawl(alias, final_url=True)
        if he.extract(new_host) != he.extract(alias):
            return
        
        # * Check if alias is a login page
        keywords = ['login']
        path = urlsplit(alias).path
        if path != "/" and path[-1] == "/": path = path[:-1]
        filename = path.split("/")[-1]
        for k in keywords:
            if k in filename.lower():
                return
        return alias