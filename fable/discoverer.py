"""
Discover backlinks to today's page
"""
import os
from urllib.parse import urlsplit, urlparse, parse_qsl, parse_qs, urlunsplit
from itertools import chain, combinations
from bs4 import BeautifulSoup
from queue import Queue
from collections import defaultdict
import re, json
import random
from dateutil import parser as dparser
import datetime

from . import config, tools, tracer
from .utils import search, crawl, text_utils, url_utils, sic_transit

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
CUT = 30

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


class Path:
    def __init__(self, url, wayback_url=None, link_sig=('', ('', '')), ss=None):
        self.url = url
        self.path = ss.path + [url] if ss else [url]
        self.wayback_path = ss.wayback_path + [wayback_url] if ss else [wayback_url]
        self.sigs = ss.sigs + [link_sig] if ss else [link_sig]
        self.length = ss.length + 1 if ss else 0
    
    def from_dict(self, d):
        self.url = d['url']
        self.path = d['path']
        self.sigs = d['sigs']
        self.length = len(self.path)
    
    def update_wayback(self, wayback_url):
        """
        Update the last array of wayback
        """
        self.wayback_path[-1] = wayback_url

    def calc_priority(self, dst, dst_rep, similar=None):
        """
        similar must be initialized to contain dst_rep and sigs
        """
        c1 = url_utils.tree_diff(dst, self.url)
        c2 = len(self.path)
        if similar:
            anchor, sig = self.sigs[-1]
            simis = (similar.tfidf.similar(anchor, dst_rep), similar.tfidf.similar(' '.join(sig), dst_rep))
            simi = wsum_simi(simis)
            self.priority = c1 + c2 - simi
        else:
            self.priority = c1 + c2
        return self
    
    def __str__(self, wayback_dump=False):
        d = {
            'url': self.url,
            'path': self.path
        }
        if wayback_dump: d.update({'wayback_path': self.wayback_path})
        return json.dumps(d, indent=2)
    
    def to_dict(self):
        return {
            'url': self.url,
            'path': self.path,
            'sigs': self.sigs,
            'wayback_path': self.wayback_path
        }


class Backpath_Finder:
    def __init__(self, policy='earliest', memo=None, similar=None):
        """
        Policy: earliest/latest
        """
        self.policy = policy
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        policy_map = {
            'earliest': 'closest-later',
            'latest': 'closest-earlier'
        }
        self.memo_policy = policy_map[self.policy]

    def find_path(self, url, homepage=None):
        policy = self.policy
        trim_size = 20
        try:
            wayback_url = self.memo.wayback_index(url, policy=policy)
            html = self.memo.crawl(wayback_url)
            content = self.memo.extract_content(html, version='domdistiller')
            title = self.memo.extract_title(html, version='domdistiller')
            url_rep = title if title != '' else content
        except Exception as e:
            tracer.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
            # Generate repr text from url given no wayback snapshot
            html, title, content = '', '', ''
            us = urlsplit(url)
            url_rep = re.sub('[^0-9a-zA-Z]+', ' ', us.path)
            trim_size = 10
            if us.query:
                values = [u[1] for u in parse_qsl(us.query)]
                url_rep += f" {' '.join(values)}"
        # TODO: Consider cases where there is no snapshot
        ts = url_utils.get_ts(wayback_url) if wayback_url else 20200101
        tracer.wayback_url(url, wayback_url)
        us = urlsplit(url)
        homepage = urlunsplit(us._replace(path='', query='', fragment='')) if not homepage else homepage
        MAX_DEPTH = len(us.path.split('/')) + len(parse_qs(us.query))
        search_queue = [Path(homepage)]
        seen = set()
        param_dict = {
            "filter": ['statuscode:[23][0-9]*', 'mimetype:text/html'],
            "collapse": "timestamp:8"
        }
        while len(search_queue) > 0:
            path = search_queue.pop(0)
            tracer.info(f'BackPath: {path.url} outgoing_queue:{len(search_queue)}')
            if len(path.path) > MAX_DEPTH or url_utils.url_norm(path.url) in seen:
                continue
            seen.add(url_utils.url_norm(path.url))
            if url_utils.url_match(url, path.url):
                return path

            wayback_url = self.memo.wayback_index(path.url, policy=self.memo_policy, ts=ts, param_dict=param_dict)
            path.update_wayback(wayback_url)
            tracer.info(wayback_url)

            if wayback_url is None:
                continue
            wayback_html, wayback_url = self.memo.crawl(wayback_url, final_url=True)
            if wayback_html is None:
                continue
            outgoing_sigs = crawl.outgoing_links_sig(wayback_url, wayback_html, wayback=True)
            self.similar.tfidf._clear_workingset()
            corpus = [s[1] for s in outgoing_sigs] + [' '.join(s[2]) for s in outgoing_sigs] + [url_rep]
            self.similar.tfidf.add_corpus(corpus)
            for wayback_outgoing_link, anchor, sib_text in outgoing_sigs:
                outgoing_link = url_utils.filter_wayback(wayback_outgoing_link)
                new_path = Path(outgoing_link, link_sig=(anchor, sib_text), ss=path)
                if url_utils.url_match(wayback_outgoing_link, url, wayback=True):
                    return new_path
                if outgoing_link not in seen:
                    new_path.calc_priority(url, url_rep, self.similar)
                    seen.add(url_utils.url_norm(outgoing_link))
                    search_queue.append(new_path)
            search_queue.sort(key=lambda x: x.priority)
            search_queue = search_queue[:trim_size] if len(search_queue) > trim_size else search_queue
    
    def wayback_alias(self, url):
        """
        Utilize wayback's archived redirections to find the alias/reorg of the page

        Returns: reorg_url is latest archive is an redirection to working page, else None
        """
        param_dict = {
            "filter": ['statuscode:[23][0-9]*', 'mimetype:text/html'],
        }
        us = urlsplit(url)
        is_homepage = us.path in ['/', ''] and not us.query
        try:
            wayback_url = self.memo.wayback_index(url, policy='latest', param_dict=param_dict)
            _, wayback_url = self.memo.crawl(wayback_url, final_url=True)
            match = url_utils.url_match(url, url_utils.filter_wayback(wayback_url))
        except:
            return
        if not match:
            new_url = url_utils.filter_wayback(wayback_url)
            new_us = urlsplit(new_url)
            new_is_homepage = new_us.path in ['/', ''] and not new_us.query
            broken, reason = sic_transit.broken(new_url, html=True, ignore_soft_404=is_homepage and new_is_homepage)
            if not broken:
                return new_url
        return

    def find_same_link(self, link_sigs, liveweb_url, liveweb_html):
        """
        For same page from wayback and liveweb (still working). Find the same url from liveweb which matches to wayback
        
        Returns: If there is a match on the url, return sig, similarity, by
                 Else: return None
        """
        live_outgoing_sigs = crawl.outgoing_links_sig(liveweb_url, liveweb_html)
        for link_sig in link_sigs:
            matched_vals = self.similar.match_url_sig(link_sig, live_outgoing_sigs)
            if matched_vals is not None:
                return matched_vals
        return

    def match_path(self, path):
        """
        Match reorganized url with the prev given path
        Return: If hit, matched_urls, by 
        """
        o_pointer, i_pointer = 0, 0
        curr_url = path.path[o_pointer]
        matched = False
        while o_pointer < len(path.path) - 1:
            i_pointer = max(i_pointer, o_pointer)
            curr_url = path.path[o_pointer] if not matched else curr_url
            tracer.info(f'curr_url: {curr_url} {matched}')
            curr_us = urlsplit(curr_url)
            is_homepage = curr_us.path in ['/', ''] and not curr_us.query
            broken, reason = sic_transit.broken(curr_url, html=True, ignore_soft_404=is_homepage)
            if broken: # Has to be not homepage to 
                new_url = self.wayback_alias(curr_url)
                if not new_url:
                    o_pointer += 1
                    matched = False
                    continue
                curr_url = new_url
            html = self.memo.crawl(curr_url)
            for compare in range(i_pointer+1, len(path.path)):
                link_match = self.find_same_link([(path.path[compare], path.sigs[compare][0], path.sigs[compare][1])], curr_url, html) # TODO, explicit list of wayback_sig mayn
                if link_match:
                    matched = True
                    matched_sig, simi, by = link_match
                    if compare == len(path.path) - 1: # Last path
                        if not sic_transit.broken(matched_sig[0])[0]:
                            return matched_sig[0], (f'link_{by}', simi)
                        else:
                            return
                    else:
                        curr_url = matched_sig[0]
                        i_pointer = compare
                        break
                else:
                    matched = False
            o_pointer += not matched
        return  


class Discoverer:
    def __init__(self, depth=DEPTH, corpus=[], proxies={}, memo=None, similar=None):
        self.depth = depth
        self.corpus = corpus
        self.PS = crawl.ProxySelector(proxies)
        self.wayback = {} # {url: wayback ts}
        self.crawled = {} # {url: html}
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.bf = Backpath_Finder(policy='latest', memo=self.memo, similar=self.similar)


    def guess_backlinks(self, url, num):
        """
        Retrieve closest neighbors for url archived by wayback
        num: Number of neighbors required

        # TODO: Add ts and url closeness into consideration
        """
        def closeness(url, cand):
            score = 0
            us, uc = urlsplit(url), urlsplit(cand)
            h1s, h2s = us.netloc.split(':')[0].split('.'), uc.netloc.split(':')[0].split('.')
            for h1, h2 in zip(reversed(h1s), reversed(h2s)):
                if h1 == h2: score += 1
                else: return score
            if len(h1s) != len(h2s):
                return score
            p1s, p2s = us.path, uc.path
            if p1s == '': p1s = '/'
            if p2s == '': p2s = '/'
            p1s, p2s = p1s.split('/')[1:], p2s.split('/')[1:]
            for p1, p2 in zip(p1s, p2s):
                if p1 == p2: score += 1
                else: break
            if len(p1s) != len(p2s):
                for _ in (len(p1s), len(p2s)): score -= 1
            q1s, q2s = parse_qs(us.query), parse_qs(uc.query)
            score += len(set(q1s.keys()).intersection(set(q2s.keys())))
            score -= max(0, len(q2s.keys()) - len( set(q1s.keys()).union( set(q2s.keys()) ) ))
            return score

        param_dict = {
            'from': 1997,
            'to': 2020,
            'filter': ['mimetype:text/html', 'statuscode:200'],
            'collapse': 'urlkey',
            'limit': 10000
        }
        cands = []
        site = he.extract(url)
        us = urlsplit(url)
        path, query = us.path, us.query
        explicit_parent = url_utils.url_parent(url)
        if self.memo.wayback_index(explicit_parent, policy='earliest'):
            cands.append(explicit_parent)
        if path not in ['', '/'] or query:
            if path and path[-1] == '/': path = path[:-1]
            path_dir = os.path.dirname(path)
            q_url = urlunsplit(us._replace(path=path_dir + '*', query=''))
            wayback_urls, _ = crawl.wayback_index(q_url, param_dict=param_dict)
        elif us.netloc.split(':')[0] != site:
            hs = us.netloc.split(':')[0].split('.')
            hs[0] = '*'
            q_url = urlunsplit(us._replace(scheme='', path='', query=''))
            wayback_urls, _ = crawl.wayback_index(q_url, param_dict=param_dict)
        else:
            # TODO Think about this
            return []
        parents = [w[1] for w in wayback_urls if url_utils.is_parent(w[1], url) and \
             not url_utils.url_match(w[1], url) and not url_utils.is_parent(w[1], explicit_parent)]
        cands += parents
        closest_urls = [w[1] for w in wayback_urls if not url_utils.url_match(w[1], url) \
            and not url_utils.url_match(w[1], explicit_parent) and w[1] not in parents and self.loop_cand(url, w[1])]
        closest_urls.sort(key=lambda x: closeness(url, x), reverse=True)
        cands += closest_urls[:max(0, num-len(parents))] if len(closest_urls) > max(0, num-len(parents)) else closest_urls
        
        return cands

    def link_same_page(self, wayback_dst, title, content, backlinked_url, backlinked_html, cut=CUT):
        """
        See whether backedlinked_html contains links to the same page as html
        content: content file of the original url want to find copy
        backlinked_html: html which could be linking to the html
        wayback_dst: In wayback form
        cut: Max number of outlinks to test on. If set to <=0, there is no limit

        Returns: (link, similarity), from_where which is a copy of html if exists. None otherwise
        """
        if backlinked_url is None:
            return None, None
        backlinked_content = self.memo.extract_content(backlinked_html, version='domdistiller')
        backlinked_title = self.memo.extract_title(backlinked_html, version='domdistiller')
        similars, fromm = self.similar.similar(wayback_dst, title, content, {backlinked_url: backlinked_title}, {backlinked_url: backlinked_content})
        if len(similars) > 0:
            return similars[0], fromm

        # outgoing_links = crawl.outgoing_links(backlinked_url, backlinked_html, wayback=False)
        global he
        outgoing_sigs = crawl.outgoing_links_sig(backlinked_url, backlinked_html, wayback=False)
        outgoing_sigs = [osig for osig in outgoing_sigs if he.extract(osig[0]) == he.extract(wayback_dst, wayback=True)]
        if cut <= 0:
            cut = len(outgoing_sigs)
        if len(outgoing_sigs) > cut:
            repr_text = [title, content]
            self.similar.tfidf._clear_workingset()
            self.similar.tfidf.add_corpus([w[1] for w in outgoing_sigs] + [' '.join(w[2]) for w in outgoing_sigs] + repr_text)
            scoreboard = defaultdict(lambda: (0, 0))
            for outlink, anchor, sig in outgoing_sigs:
                simis = (self.similar.max_similar(anchor, repr_text, init=False)[0], self.similar.max_similar(' '.join(sig), repr_text, init=False)[0])
                scoreboard[outlink] = max(scoreboard[outlink], simis, key=lambda x: wsum_simi(x))
            scoreboard = sorted(scoreboard.items(), key=lambda x: wsum_simi(x[1]), reverse=True)
            outgoing_links = [sb[0] for sb in scoreboard[:cut]]
        else:
            outgoing_links = [osig[0] for osig in outgoing_sigs]

        # outgoing_contents = {}
        for outgoing_link in outgoing_links:
            if he.extract(wayback_dst, wayback=True) != he.extract(outgoing_link):
                continue
            html = self.memo.crawl(outgoing_link, proxies=self.PS.select())
            if html is None: continue
            tracer.info(f'Test if outgoing link same: {outgoing_link}')
            outgoing_content = self.memo.extract_content(html, version='domdistiller')
            outgoing_title = self.memo.extract_title(html, version='domdistiller')
            similars, fromm = self.similar.similar(wayback_dst, title, content, {outgoing_link: outgoing_title}, {outgoing_link: outgoing_content})
            if len(similars) > 0:
                return similars[0], fromm
        return None, None
    
    def find_same_link(self, wayback_sigs, liveweb_url, liveweb_html):
        """
        For same page from wayback and liveweb (still working). Find the same url from liveweb which matches to wayback
        
        Returns: If there is a match on the url, return sig, similarity, by
                 Else: return None
        """
        live_outgoing_sigs = crawl.outgoing_links_sig(liveweb_url, liveweb_html)
        for wayback_sig in wayback_sigs:
            matched_vals = self.similar.match_url_sig(wayback_sig, live_outgoing_sigs)
            if matched_vals is not None:
                return matched_vals
        return
    
    def loop_cand(self, url, outgoing_url):
        """
        See whether outgoing_url is worth looping through
        Creteria: 1. Same domain 2. No deeper than url
        """
        global he
        if 'web.archive.org' in outgoing_url:
            outgoing_url = url_utils.filter_wayback(outgoing_url)
        if he.extract(url) != he.extract(outgoing_url):
            return False
        if urlsplit(url).path in urlsplit(outgoing_url).path and urlsplit(url).path != urlsplit(outgoing_url).path:
            return False
        return True

    def _wayback_alias(self, url):
        """
        Old version of wayback_alias, used for internal alias found
        Utilize wayback's archived redirections to find the alias/reorg of the page

        Returns: reorg_url is latest archive of an redirection to working page, else None
        """
        tracer.debug(f'_wayback_alias: {url}')
        param_dict = {
            "filter": ['statuscode:[23][0-9]*', 'mimetype:text/html'],
        }
        us = urlsplit(url)
        is_homepage = us.path in ['/', ''] and not us.query
        try:
            wayback_url = self.memo.wayback_index(url, policy='latest', param_dict=param_dict)
            _, wayback_url = self.memo.crawl(wayback_url, final_url=True)
            match = url_utils.url_match(url, url_utils.filter_wayback(wayback_url))
        except:
            return
        if not match:
            new_url = url_utils.filter_wayback(wayback_url)
            new_us = urlsplit(new_url)
            new_is_homepage = new_us.path in ['/', ''] and not new_us.query
            if new_is_homepage and (not is_homepage): 
                return
            broken, reason = sic_transit.broken(new_url, html=True, ignore_soft_404=is_homepage and new_is_homepage)
            if not broken:
                return new_url
        return

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
        param_dict = {
            "filter": ['statuscode:[23][0-9]*', 'mimetype:text/html'],
        }
        us = urlsplit(url)
        is_homepage = us.path in ['/', ''] and not us.query
        try:
            wayback_ts_urls = self.memo.wayback_index(url, policy='all', param_dict=param_dict)
        except: return

        if not wayback_ts_urls or len(wayback_ts_urls) == 0:
            return

        wayback_ts_urls = [(dparser.parse(c[0]), c[1]) for c in wayback_ts_urls]
        # * Count for unmatched wayback final url, and wayback_alias to same redirected fake alias
        url_match_count, same_redir = 0, 0
        it = len(wayback_ts_urls) - 1
        last_ts = wayback_ts_urls[-1][0] + datetime.timedelta(days=90)
        seen_redir_url = set()


        def verify_alias(url, new_urls, ts, homepage_redir, strict_filter):
            """
            Verify whether new_url is valid alias by checking:
             1. new_urls is working 
             2. whether there is no other url in the same form redirected to this url
            """
            global tracer
            nonlocal seen_redir_url, require_neighbor
            
            # *If homepage to homepage redir, no soft-404 will be checked
            new_url = new_urls[-1]
            broken, _ = sic_transit.broken(new_url, html=True, ignore_soft_404=homepage_redir)
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
                "filter": ['statuscode:[23][0-9]*', 'mimetype:text/html'],
                'limit': 100
            }
            tracer.debug(f'Search for neighbors with query & year: {url_prefix} {ts_year}')
            neighbor, _ = crawl.wayback_index(url_prefix, param_dict=param_dict, total_link=True)

            # *Get closest crawled urls in the same dir, which is not target itself  
            lambda_func = lambda u: not url_utils.url_match(url, url_utils.filter_wayback(u) ) # and not url_utils.filter_wayback(u)[-1] == '/'
            lambda_func2 = lambda u: url_utils.nondigit_dirname(urlsplit(url_utils.filter_wayback(u)).path[:-1]) in url_dir
            neighbor = sorted([n for n in neighbor if lambda_func(n[1]) and lambda_func2(n[1])], key=lambda x: abs((dparser.parse(x[0]) - ts).total_seconds()))
            tracer.debug(f'neightbor: {len(neighbor)}')
            match = require_neighbor
            if len(neighbor) <= 0:
                return not match  # * If there is no neighbor, return result depend on whether require_neighbor is necessary
            for i in range(min(5, len(neighbor))):
                try:
                    tracer.debug(f'Choose closest neighbor: {neighbor[i][1]}')
                    response = crawl.requests_crawl(neighbor[0][1], raw=True)
                    neighbor_urls = [r.url for r in response.history[1:]] + [response.url]
                    match = False
                    for neighbor_url in neighbor_urls:
                        for new_url in new_urls:    
                            thismatch = url_utils.url_match(new_url, neighbor_url)
                            if thismatch: 
                                match = True
                                seen_redir_url.add(new_url)
                    break
                except Exception as e:
                    tracer.debug(f'Cannot check neighbor on wayback_alias')
                    continue
            if match:
                tracer.debug(f'url in same dir: {neighbor[0][1]} redirects to the same url')
            return not match

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
                # //pass_check, reason = sic_transit.broken(new_url, html=True, ignore_soft_404=is_homepage and new_is_homepage)
                # //ass_check = not pass_check
                if len(inter_urls) > 1:
                    inter_urls = inter_urls[1:]
                pass_check = verify_alias(url, inter_urls, ts, homepage_redir=is_homepage and new_is_homepage, strict_filter=strict_filter)
                if pass_check:
                    return new_url
            else:
                url_match_count += 1
        return

    def discover_backlinks(self, src, dst, dst_title, dst_content, dst_html, dst_snapshot, dst_ts=None):
        """
        For src and dst, see:
            1. If src is archived on wayback
            2. If src is linking to dst on wayback
            3. If src is still working today
        
        dst_ts: timestamp for dst on wayback to reference on policy
                If there is no snapshot, use closest-latest
        
        returns: 
            {
                "status": found/loop/reorg/notfound,
                "url(s)": found: found url,
                          loop: outgoing loop urls,
                "links":  found: matched backlink
                          reorg/notfound: backlink
                "reason": found: (from, similarity, how)
                          notfound: (reason why not found)

                "wayback_src": wayback url of src
            }       
        """
        tracer.info(f'Backlinks: {src} {dst}')
        policy = 'closest' if dst_ts else 'latest-rep'
        wayback_dst = url_utils.constr_wayback(dst, dst_ts) if dst_ts else url_utils.constr_wayback(dst, '20201231')

        param_dict = {
            "filter": ['statuscode:[23][0-9]*', 'mimetype:text/html'],
            "collapse": "timestamp:8"
        }
        r_dict = {
            "status": None
        }
        wayback_src = self.memo.wayback_index(src, policy=policy, ts=dst_ts, param_dict=param_dict)
        r_dict['wayback_src'] = wayback_src

        # TODO(eff): Taking long time, due to crawl
        src_broken, reason = sic_transit.broken(src, html=True)
        tracer.debug(f"Check breakage of src: {src}")

        # *Directly check this outgoing page
        if not src_broken:
            src_html = self.memo.crawl(src)
            src_content = self.memo.extract_content(src_html, version='domdistiller')
            src_title = self.memo.extract_title(src_html, version='domdistiller')
            similars, fromm = self.similar.similar(wayback_dst, dst_title, dst_content, {src: src_title}, {src: src_content})
            if len(similars) > 0:
                tracer.info(f'Discover: Directly found copy during looping')
                top_similar = similars[0]
                r_dict.update({
                    "status": "found",
                    "url(s)": top_similar[0],
                    "reason": (f'{fromm}', top_similar[1], 'matched on backcand page')
                })
                return r_dict

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
        if src_broken:
            new_src = self._wayback_alias(src)
            if new_src:
                src_broken = False
                src = new_src 
        if wayback_linked[0] and not src_broken: # * src linking to dst and is working today
            src_html, src = self.memo.crawl(src, final_url=True, max_retry=2)
            rval = self.find_same_link(wayback_linked[1], src, src_html)
            if rval:
                matched_sig, simi, by = rval
                if not sic_transit.broken(matched_sig[0])[0]:
                    r_dict.update({
                        "status": "found",
                        "links": matched_sig,
                        "url(s)": matched_sig[0],
                        "reason": (f'link_{by}', simi, 'matched backlinks')
                    })
                    return r_dict
                else:
                    r_dict.update({
                        "status": "notfound",
                        "links": wayback_linked[1],
                        "reason": "Linked, matched url broken"
                    })
                    return r_dict
            elif dst_snapshot: # * Only consider the case for dst with snapshots
                top_similar, fromm = self.link_same_page(wayback_dst, dst_title, dst_content, src, src_html)
                if top_similar is not None:
                    r_dict.update({
                        "status": "found",
                        "url(s)": top_similar[0],
                        "reason": (fromm, top_similar[1], 'matched on outgoing links')
                    })
                    return r_dict
                else:
                    r_dict.update({
                        "status": "notfound",
                        "links": wayback_linked[1],
                        "reason": "Linked, no matched link"
                    })
                    return r_dict
            else: # *For dst without snapshots
                r_dict.update({
                    "status": "notfound",
                    "links": wayback_linked[1],
                    "reason": "Linked, no matched link"
                })
                return r_dict
        elif not wayback_linked[0]: # *Not linked to dst, need to look futher
            r_dict.update({
                "status": "loop",
                "url(s)": wayback_outgoing_sigs,
                "reason": None
            })
            return r_dict
        else: # *Linked to dst, but broken today
            r_dict.update({
                "status": "reorg",
                "url(s)": wayback_outgoing_sigs,
                "reason": "Backlink broken today"
            })
            return r_dict

    def discover(self, url, depth=None, seen=None, trim_size=TRIM_SIZE):
        """
        Discover the potential reorganized site
        Trim size: The largest size of outgoing queue

        Return: If Found: URL, Trace (whether it information got suffice, how copy is found, etc)
                else: None, {'suffice': Bool, 'trace': traces}
        """
        if depth is None: depth = self.depth
        has_snapshot = False
        url_ts = None
        suffice = False # *Only used for has_snapshot=False. See whehter url sufice restrictions. (Parent sp&linked&not broken today)
        traces = [] # *Used for tracing the discovery process
        repr_text = [] # *representitive text for url, composed with [title, content, url]
        ### *First try with wayback alias
        try:
            wayback_url = self.memo.wayback_index(url, policy='latest-rep')
            html, wayback_url = self.memo.crawl(wayback_url, final_url=True)
            content = self.memo.extract_content(html, version='domdistiller')
            title = self.memo.extract_title(html, version='domdistiller')
            url_ts = url_utils.get_ts(wayback_url)
            has_snapshot = True
            tracer.wayback_url(url, wayback_url)
        except Exception as e:
            tracer.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
            html, title, content = '', '', ''
        suffice = has_snapshot or suffice

        # *Get repr_text
        repr_text += [title, content]
        us = urlsplit(url)
        url_text = re.sub('[^0-9a-zA-Z]+', ' ', us.path)
        if us.query:
            values = [u[1] for u in parse_qsl(us.query)]
            url_text += f" {' '.join(values)}"
        repr_text.append(url_text)
        # End

        guess_total = defaultdict(int)
        g, curr_url = GUESS_DEPTH, url
        while g > 0:
            guessed_urls = self.guess_backlinks(curr_url, num=g)
            for gu in guessed_urls: guess_total[gu] = max(guess_total[gu], depth) # TODO depth can be changed with distance to url
            curr_url = url_utils.url_parent(curr_url)
            g -= 1

        seen = set() if seen is None else seen
        # seen.update(guessed_urls)

        guess_total = list(guess_total.items())

        outgoing_queue = []
        if has_snapshot: # *Only loop to find backlinks when snapshot is available
            outgoing_sigs = crawl.outgoing_links_sig(wayback_url, html, wayback=True)
            self.similar.tfidf._clear_workingset()
            self.similar.tfidf.add_corpus([w[1] for w in outgoing_sigs] + [' '.join(w[2]) for w in outgoing_sigs] + repr_text)
            scoreboard = defaultdict(int)
            for outlink, anchor, sig in outgoing_sigs:
                outlink = url_utils.filter_wayback(outlink)
                # *For each link, find highest link score
                if outlink not in seen and self.loop_cand(url, outlink):
                    simis = (self.similar.max_similar(anchor, repr_text, init=False)[0], self.similar.max_similar(' '.join(sig), repr_text, init=False)[0])
                    spatial = url_utils.tree_diff(url, outlink)
                    scoreboard[outlink] = max(scoreboard[outlink], estimated_score(spatial, simis))
            for outlink, score in scoreboard.items():
                outgoing_queue.append((outlink, depth-OUTGOING, score))

            outgoing_queue.sort(key=lambda x: x[2])
            outgoing_queue = outgoing_queue[: trim_size+1] if len(outgoing_queue) >= trim_size else outgoing_queue

        ee_count = 2 * (not has_snapshot) # Early exit satisfy condition score
        def early_exit(status, reason):
            """Determine whether it is ok to early exit"""
            nonlocal ee_count
            if status != "notfound":
                return False
            if reason == "backlink no snapshot":
                ee_count += 1
            return ee_count > 2

        while len(guess_total) + len(outgoing_queue) > 0:
            # *Ops for guessed links
            two_src = []
            if len(guess_total) > 0:
                two_src.append(guess_total.pop(0))
            if len(outgoing_queue) > 0:
                two_src.append(outgoing_queue.pop(0)[:2])
            for item in two_src:
                src, link_depth = item
                tracer.info(f"Got: {src} depth:{link_depth} guess_total:{len(guess_total)} outgoing_queue:{len(outgoing_queue)}")
                if src in seen:
                    continue
                seen.add(src)
                r_dict = self.discover_backlinks(src, url, title, content, html, has_snapshot, url_ts)
                status, reason = r_dict['status'], r_dict['reason']
                tracer.discover(url, src, r_dict.get("wayback_src"), status, reason, r_dict.get("links"))
                if early_exit(status, reason):
                    break
                if status == 'found':
                    return r_dict['url(s)'], {'suffice': True, 'type': reason[0], 'value': reason[1]}
                elif status == 'loop':
                    out_sigs = r_dict['url(s)']
                    if link_depth >= OUTGOING:
                        scoreboard = defaultdict(int)
                        self.similar.tfidf._clear_workingset()
                        self.similar.tfidf.add_corpus([w[1] for w in out_sigs] + [' '.join(w[2]) for w in out_sigs] + repr_text)
                        for outlink, anchor, sig in out_sigs:
                            outlink = url_utils.filter_wayback(outlink)
                            # For each link, find highest link score
                            if outlink not in seen and self.loop_cand(url, outlink):
                                simis = (self.similar.max_similar(anchor, repr_text, init=False)[0], self.similar.max_similar(' '.join(sig), repr_text, init=False)[0])
                                spatial = url_utils.tree_diff(url, outlink)
                                scoreboard[outlink] = max(scoreboard[outlink], estimated_score(spatial, simis))
                        for outlink, score in scoreboard.items():
                            outgoing_queue.append((outlink, link_depth-OUTGOING, score))
                elif status in ['notfound', 'reorg']:
                    reason = r_dict['reason']
                    if not has_snapshot:
                        suffice = suffice or 'Linked' in reason
            
            # *Trim low-rank urls, and do deduplications
            outgoing_queue.sort(key=lambda x: x[2])
            dedup,uniq_q, uniq_c = set(), [], 0
            while len(dedup) < trim_size and uniq_c < len(outgoing_queue):
                if outgoing_queue[uniq_c][0] not in dedup:
                    dedup.add(outgoing_queue[uniq_c][0])
                    uniq_q.append(outgoing_queue[uniq_c])
                uniq_c += 1
            outgoing_queue = uniq_q

        return None, {'suffice': suffice}

    def bf_find(self, url, policy='latest'):
        """
        Return: If Hit, reorg_url, {'suffice': , 'type': , 'value': , 'backpath': path}
                Else: None, {'suffice': False, 'backpath': depends} 
        """
        self.bf.policy = policy
        if urlsplit(url).path in {'', '/'}:
            return None, {'suffice': False}
        path = self.bf.find_path(url)
        if not path:
            return None, {'suffice': False}
        tracer.backpath_findpath(url, path)
        rval = self.bf.match_path(path)
        if rval:
            reorg_url, (typee, value) = rval
            return reorg_url, {'suffice': True, 'type': typee, 'value': value, 'backpath': path.to_dict()}
        else:
            return None, {'suffice': True, 'backpath': path.to_dict()}
        