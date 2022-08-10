"""
Discover backlinks to today's page
"""
import os
from urllib.parse import urlsplit, urlparse, parse_qsl, parse_qs, urlunsplit
from bs4 import BeautifulSoup
from queue import Queue
from collections import defaultdict
import re, json
from dateutil import parser as dparser
import datetime

from fable import config, tools, tracer
from fable.utils import crawl, url_utils, sic_transit

import logging
logging.setLoggerClass(tracer.tracer)
tracer = logging.getLogger('logger')
logging.setLoggerClass(logging.Logger)

he = url_utils.HostExtractor()

DEPTH = 4 # Depth for searching backlinks
TRIM_SIZE = 10 # trim size for the queue
GUESS_DEPTH = 3
OUTGOING = 1 # Single cost if a an iteration
MIN_GUESS = 6
CUT = 10

def wsum_simi(simi):
    return 3/4*simi[0] + 1/4*simi[1]

def estimated_score(spatial, simi):
    """
    similarity consider as the possibilities of hitting in next move
    Calculated as kp*1 + (1-kp)*n 
    """
    p = 3/4*simi[0] + 1/4*simi[1]
    k = 1
    return k*p + (1-k*p)*spatial

class Discoverer:
    def __init__(self, depth=DEPTH, corpus=[], proxies={}, memo=None, similar=None):
        self.depth = depth
        self.corpus = corpus
        self.PS = crawl.ProxySelector(proxies)
        self.wayback = {} # {url: wayback ts}
        self.crawled = {} # {url: html}
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.threshold = 0.8


    def guess_backlinks(self, url, level=1):
        """
        Retrieve closest neighbors for url archived by wayback
        level: How many level for parents to query

        # TODO: Add ts and url closeness into consideration
        """
        param_dict = {
            'from': 1997,
            'to': 2021,
            'filter': ['mimetype:text/html', 'statuscode:200'],
            # 'collapse': 'urlkey',
            'limit': 50000
        }
        cands = []
        site = he.extract(url)
        cur_url = url
        for _ in range(level):
            us = urlsplit(cur_url)
            path, query = us.path, us.query
            if path not in ['', '/'] or query:
                if path and path[-1] == '/': path = path[:-1]
                path_dir = os.path.dirname(path)
                cur_url = urlunsplit(us._replace(path=path_dir, query=''))
            elif us.netloc.split(':')[0] != site:
                hs = us.netloc.split(':')[0].split('.')
                cur_url = urlunsplit(us._replace(netloc='.'.join(hs[1:]), path='', query=''))
            else:
                # TODO Think about this
                return []
        wayback_urls, _ = crawl.wayback_index(os.path.join(cur_url, '*'), param_dict=param_dict, total_link=True)
        parents = [w[1] for w in wayback_urls if not url_utils.url_match(w[1], url)]
        cands += parents
        return cands

    def _find_same_link(self, wayback_sigs, backlink_sigs):
        """
        For same page from wayback and liveweb (still working). Find the same url from liveweb which matches to wayback
        
        Returns: 1st & 2nd highest match
        """
        max_match = [('', '', 0), ('', '', 0)] # * [(URL, anchor text, Similarity)]
        # * Whehter 1st and 2nd simi are separable
        separable = lambda x: x[0][2] >= self.threshold and x[1][2] < self.threshold
        # * Whehther 1st and 2nd simi are both similar
        duplicate = lambda x: x[0][2] >= self.threshold and x[1][2] >= self.threshold
        for wayback_sig in wayback_sigs:
            matched_vals = self.similar.match_url_sig(wayback_sig, backlink_sigs)
            anchor_vals = sorted(matched_vals["anchor"].values(), key=lambda x: x[2], reverse=True)
            while len(anchor_vals) < 2: anchor_vals.append(('', '', 0))
            if separable(anchor_vals):
                max_match = anchor_vals[:2] if anchor_vals[0][2] > max_match[0][2] else max_match
            elif duplicate(anchor_vals):
                sig_vals = sorted([matched_vals["sig"].get(v[0], (v[0], "", 0)) for v in anchor_vals if v[2] >= self.threshold], key=lambda x: x[2], reverse=True)
                while len(sig_vals) < 2: sig_vals.append(('', '', 0))
                if separable(sig_vals):
                    max_match = sig_vals[:2] if sig_vals[0][2] > max_match[0][2] else max_match
        return max_match
    
    def _first_not_linked(self, dst, src, waybacks):
        """
        Find first archived copy of backlink that don't have link to the dst URL (including liveweb version if available):
        Need to see consecutive non exist backlink
        At most check 1 snapshot for each month

        dst: target URL (in liveweb form)
        src: Start wayback URL for backlink archive
        waybacks: wayback URLs of src

        Return: If there is such link, return [sigs], else, None
        """
        months = set()
        ts_src = url_utils.get_ts(src)
        waybacks = [w[1] for w in waybacks if ts_src < w[0]]
        if not sic_transit.broken(url_utils.filter_wayback(src))[0]:
            waybacks.append(url_utils.filter_wayback(src))
        last_not_seen, last_sigs = None, None
        for wayback in waybacks:
            ts = url_utils.get_ts(wayback)
            if not ts: ts = '20211231'
            month = ts[:6]
            if month in months: continue
            months.add(month)
            wayback_html = self.memo.crawl(wayback)
            wayback_outgoing_sigs = crawl.outgoing_links_sig(wayback, wayback_html, wayback=True)
            # TODO: Backlink anchor text may evolve during different copies
            seen = False
            for wayback_outgoing_link, anchor, sibtext in wayback_outgoing_sigs:
                if url_utils.url_match(wayback_outgoing_link, dst, wayback=True):
                    seen = True
                    break
            if seen:
                continue
            tracer.debug(f"_first_not_linked: {src} has last not linked copy: {wayback}")
            if last_not_seen:
                break
            else:
                last_not_seen = wayback
                last_sigs = wayback_outgoing_sigs
        return last_sigs

    def discover_backlinks(self, wayback_src, dst):
        """
        Naive discover backlinks
        
        returns: 
            {
                "status": found/loop/NoDrop/NoMatch/Broken,
                "url(s)": found: found url,
                          loop: outgoing loop urls,
                "links":  found: matched backlink
                          Others: backlink
                "reason": found: (from, similarity, how)
                          Others: (reason why not found)

                "wayback_src": wayback url of src
            }       
        """
        tracer.info(f'Backlinks: {wayback_src} {dst}')
        src = url_utils.filter_wayback(wayback_src)

        r_dict = {
            "status": None
        }
        r_dict['wayback_src'] = wayback_src

        # *No archive in wayback for src url (usually is guessed_url)
        if wayback_src is None:
            return {
                "status": "notfound",
                "link": None,
                "reason": "backlink no snapshot",
                "wayback_src": None
            }
        
        wayback_src_html, wayback_src = self.memo.crawl(wayback_src, final_url=True)
        wayback_outgoing_sigs = crawl.outgoing_links_sig(wayback_src, wayback_src_html, wayback=True)
        wayback_linked = [False, []]
        for wayback_outgoing_link, anchor, sibtext in wayback_outgoing_sigs:
            if url_utils.url_match(wayback_outgoing_link, dst, wayback=True):
                # TODO: linked can be multiple links
                wayback_linked[0] = True
                wayback_linked[1].append((wayback_outgoing_link, anchor, sibtext))
        tracer.info(f'Wayback linked: {wayback_linked[1]}')

        # * To mach a link, require to pass 4 stage: 
        # * linked, (original link) dropped, (new link) matched, (new URL) work
        if wayback_linked[0]: # * 1.1 linked
            r_dict.update({
                "status": "Linked",
                "url(s)": wayback_src,
                "reason": "Linked, Only check for backlink"
            })
            # all_wayback_src = self.memo.wayback_index(src, policy="all")
            # backlink_sigs = self._first_not_linked(dst, wayback_src, all_wayback_src)
            # if backlink_sigs: # * 2.1 dropped
            #     max_match = self._find_same_link(wayback_linked[1], backlink_sigs)
            #     if max_match[0][2] >= self.threshold and max_match[1][2] < self.threshold: # * 3.1 matched
            #         top_match = max_match[0]
            #         if not sic_transit.broken(max_match[0][0], html=True)[0]: # * 4.1 work
            #             fromm = "anchor" if isinstance(top_match[1], str) else "sig"
            #             r_dict.update({
            #                 "status": "found",
            #                 "url(s)": max_match[0][0],
            #                 "reason": (fromm, top_match[1], 'matched on backlinks')
            #             })
            #         else: # * 4.0 Not working
            #             r_dict.update({
            #                 "status":  "Broken",
            #                 "url(s)": max_match[0][0],
            #                 "reason": "Matched new link's URL still broken"
            #             })
            #     else: # * 3.0 Not matched, link same page
            #         r_dict.update({
            #             "status": "NoMatch",
            #             "links": wayback_linked[1],
            #             "reason": "Linked, no matched link"
            #         })
            # else: # * 2.0 Not dropped
            #     r_dict.update({
            #         "status": "NoDrop",
            #         "links": wayback_linked[1],
            #         "reason": "Linked, never (detected) drop the old link"
            #     })
        else: # * 1.0 Not linked to dst, need to look futher
            r_dict.update({
                "status": "loop",
                "url(s)": wayback_outgoing_sigs,
                "reason": None
            })
        return r_dict

    def discover(self, url, total_budget, seen=None):
        """
        Discover the potential reorganized site
        Trim size: The largest size of outgoing queue

        Return: If Found: URL, Trace (whether it information got suffice, how copy is found, etc)
                else: None, {'suffice': Bool, 'trace': traces}
        """
        seen = set() if seen is None else seen
        us = urlsplit(url)
        # * Path
        max_levels = len(list(filter(lambda x: x!='', us.path.split('/'))))
        # * Site
        max_levels += len(us.netloc.split(':')[0].split('.')) - len(he.extract(url).split('.'))
        iteration, level = 0, 1
        guess_total = []
        while iteration < total_budget:
            while len(guess_total) <= 0:
                if level > max_levels:
                    break
                guess_total = self.guess_backlinks(url, level=level)
                level += 1
                # TODO: Whether guess links could exhaust before Budgets
            if len(guess_total) <= 0:
                break
            src = guess_total.pop(0)
            tracer.info(f"Got: {src} iteration: {iteration} budget: {total_budget}")
            if src in seen:
                continue
            seen.add(src)
            iteration += 1
            r_dict = self.discover_backlinks(src, url)

            status, reason = r_dict['status'], r_dict['reason']
            tracer.discover(url, src, r_dict.get("wayback_src"), status, reason, r_dict.get("links"))
            if status == 'Linked':
                return r_dict['url(s)'], {}
            elif status == 'loop':
                out_sigs = r_dict['url(s)']
            
            # if status == 'found':
            #     return r_dict['url(s)'], {'suffice': True, 'type': reason[0], 'value': reason[1]}
            # elif status == 'loop':
            #     out_sigs = r_dict['url(s)']
            # elif status in ['notfound', 'reorg']:
            #     reason = r_dict['reason']

        return None, {'suffice': 'N/A'}