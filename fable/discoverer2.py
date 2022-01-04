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

from . import config, tools, tracer
from .utils import crawl, url_utils, sic_transit

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


    def guess_backlinks(self, url, num, ts=None):
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
        
        if ts:
            year = dparser.parse(ts).year
            start = year - 2
            end = year + 2
        else:
            start = 1997
            end = 2022
        param_dict = {
            'from': start,
            'to': end,
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
        # * Add homepage to candidates
        home_us = us._replace(path='/', fragment='', query='')
        cands.append(urlunsplit(home_us))
        if path not in ['', '/'] or query:
            if path and path[-1] == '/': path = path[:-1]
            path_dir = os.path.dirname(path)
            q_url = urlunsplit(us._replace(path=path_dir + '*', query=''))
            wayback_urls, _ = crawl.wayback_index(q_url, param_dict=param_dict)
        elif us.netloc.split(':')[0] != site:
            hs = us.netloc.split(':')[0].split('.')
            hs[0] = '*'
            q_url = urlunsplit(us._replace(netloc='.'.join(hs), path='', query=''))
            wayback_urls, _ = crawl.wayback_index(q_url, param_dict=param_dict)
        else:
            # TODO Think about this
            return []
        parents = list(set([url_utils.url_norm(w[1]) for w in wayback_urls if url_utils.is_parent(w[1], url) and \
             not url_utils.url_match(w[1], url) and not url_utils.is_parent(w[1], explicit_parent)]))
        cands += parents
        closest_urls = list(set([url_utils.url_norm(w[1]) for w in wayback_urls if not url_utils.url_match(w[1], url) \
            and not url_utils.url_match(w[1], explicit_parent) and w[1] not in parents and self.loop_cand(url, w[1])]))
        closest_urls.sort(key=lambda x: closeness(url, x), reverse=True)
        cands += closest_urls[:max(0, num-len(parents))] if len(closest_urls) > max(0, num-len(parents)) else closest_urls
        return cands

    def _link_same_page(self, wayback_dst, title, content, backlinked_url, backlinked_html, cut=CUT):
        """
        See whether backlinked_html contains links to the same page as html
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
    
    def _first_not_linked(self, dst, src, waybacks):
        """
        Find first archived copy of backlink that don't have link to the dst URL (including liveweb version if available):
        Need to see consecutive non exist backlink
        At most check 1 snapshot for each month

        dst: target URL (in liveweb form)
        src: Start wayback URL for backlink archive (in wayback form)
        waybacks: wayback URLs of src

        Return: If there is such link, return ([sigs], ts) else, (None, dummy_ts)
        """
        months = set()
        live_ts = '20211231'
        ts_src = url_utils.get_ts(src)
        waybacks = [w[1] for w in waybacks if ts_src < w[0]]
        live_src = url_utils.filter_wayback(src)
        if not sic_transit.broken(live_src)[0]:
            _, live_src = self.memo.crawl(live_src, final_url=True)
            waybacks.append(live_src)
        last_not_seen, last_sigs, last_ts = None, None, ts_src
        for wayback in waybacks:
            ts = url_utils.get_ts(wayback)
            if not ts: ts = live_ts
            month = ts[:6]
            if month in months: continue
            months.add(month)
            wayback_html = self.memo.crawl(wayback)
            wayback_outgoing_sigs = crawl.outgoing_links_sig(wayback, wayback_html, wayback=True)
            # TODO: Backlink anchor text may evolve during different copies
            seen = False
            for wayback_outgoing_link, anchor, sibtext in wayback_outgoing_sigs:
                # print(wayback_outgoing_link, dst)
                if url_utils.url_match(wayback_outgoing_link, dst, wayback=True):
                    seen = True
                    break
            if seen:
                continue
            tracer.debug(f"_first_not_linked: {url_utils.filter_wayback(src)} has copy not linking dst: {wayback}")
            if last_not_seen:
                break
            else:
                last_not_seen = wayback
                last_sigs = wayback_outgoing_sigs
                last_ts = ts if ts != live_ts else 'livets'
        return last_sigs, last_ts

    def _find_same_link(self, wayback_sigs, backlink_sigs):
        """
        For same page from wayback and liveweb (still working). Find the same url from liveweb which matches to wayback
        
        Returns: 1st & 2nd highest match in the form of [(URL, anchor text, Similarity)]
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
        us = urlsplit(url)
        is_homepage = us.path in ['/', ''] and not us.query
        try:
            wayback_url = self.memo.wayback_index(url, policy='latest', all_none_400=True)
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
    
    def discover_backlinks(self, src, dst, dst_title, dst_content, dst_html, dst_ts=None):
        """
        For src and dst, to match a page, check 4 requirements:
            1. If src linked to original dst on wayback
            2. If src dropped the original link (either on wayback or livepage)
            3. If a single new link matches the original link
            4. If URL in the matched new link is still working today
        
        dst_ts: timestamp for dst on wayback to reference on policy
                If there is no snapshot, use closest-latest
        
        returns: 
            {
                "status": found/loop/NoDrop/NoMatch/Broken,
                "url(s)": found/Broken: found url,
                          loop: outgoing loop urls,
                "reason": found: (from, similarity, how)
                          Others: (reason why not found)
                "archive": archived backlinks
                "live":   found: today's matched backlink

                "wayback_src": wayback url of src
            }       
        """
        tracer.info(f'Backlinks: {src} {dst}')
        policy = 'closest' if dst_ts else 'latest-rep'
        wayback_dst = url_utils.constr_wayback(dst, dst_ts) if dst_ts else url_utils.constr_wayback(dst, '20211231')


        r_dict = {
            "status": None
        }
        wayback_src = self.memo.wayback_index(src, policy=policy, ts=dst_ts, all_none_400=True)
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
        # if src_broken:
        #     new_src = self._wayback_alias(src)
        #     if new_src:
        #         src_broken = False
        #         src = new_src 
        # * To mach a link, require to pass 4 stage: 
        # * linked, (original link) dropped, (new link) matched, (new URL) work
        if wayback_linked[0]: # * 1.1 linked
            all_wayback_src = self.memo.wayback_index(src, policy="all")
            drop_versions = [all_wayback_src, []] # * Check 2 versions: 1). Wayback drop, 2). Liveweb drop
            for dv in drop_versions:
                backlink_sigs, end_ts = self._first_not_linked(dst, wayback_src, dv)
                # * 2.1 dropped
                if backlink_sigs:
                    max_match = self._find_same_link(wayback_linked[1], backlink_sigs)
                    # * 3.1 matched
                    if max_match[0][2] >= self.threshold and max_match[1][2] < self.threshold:
                        top_match = max_match[0]
                        # * 4.1 work
                        if not sic_transit.broken(max_match[0][0], html=True)[0]:
                            fromm = "anchor" if isinstance(top_match[1], str) else "sig"
                            r_dict.update({
                                "status": "found",
                                "url(s)": top_match[0],
                                "reason": (fromm, top_match[2], 'matched on backlinks'),
                                "archive": {
                                    "ts": url_utils.get_ts(wayback_src),
                                    "links": wayback_linked[1],
                                },
                                "live": {
                                    'ts': end_ts,
                                    'links': top_match[1]
                                }
                            })
                            break
                        else: # * 4.0 Not working
                            r_dict.update({
                                "status":  "Broken",
                                "reason": "Matched new link's URL still broken",
                                "url(s)": top_match[0],
                                "archive": {"links": wayback_linked[1]}
                            })
                    else: # * 3.0 Not matched, link same page
                        continue
                        top_similar, fromm = None, None
                        if not src_broken:
                            top_similar, fromm = self._link_same_page(wayback_dst, dst_title, dst_content, src, src_html)
                        if top_similar is not None:
                            r_dict.update({
                                "status": "found",
                                "url(s)": top_similar[0],
                                "reason": (fromm, top_similar[1], 'matched on blind outgoing links')
                            })
                        else:
                            r_dict.update({
                                "status": "NoMatch",
                                "reason": "Linked, no matched link",
                                "archive": {"links": wayback_linked[1]}
                            })
                        r_dict.update({
                            "status": "NoMatch",
                            "reason": "Linked, no matched link",
                            "archive": {"links": wayback_linked[1]}
                        })
                else: # * 2.0 Not dropped
                    r_dict.update({
                        "status": "NoDrop",
                        "reason": "Linked, never (detected) drop the old link",
                        "archive": {"links": wayback_linked[1]}
                    })
                    break
        else: # * 1.0 Not linked to dst, need to look futher
            r_dict.update({
                "status": "loop",
                "url(s)": wayback_outgoing_sigs,
                "reason": None
            })
        return r_dict

    def _check_archive_canonical(self, url, html):
        """
        Check whether archive has the working canonical
        url: wayback form

        Return: alias if found, else None
        """
        canonical = crawl.get_canonical(url, html)
        if not url_utils.url_match(url, canonical, wayback=True):
            canonical = url_utils.filter_wayback(canonical)
            if sic_transit.broken(canonical)[0] is False:
                return canonical
        return

    def discover(self, url, depth=None, seen=None, trim_size=TRIM_SIZE):
        """
        Discover the potential reorganized site
        Trim size: The largest size of outgoing queue

        Return: If Found: URL, Trace (how copy is found)
                else: None, Trace
        """
        if depth is None: depth = self.depth
        has_snapshot = False
        url_ts = None
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
        
        # * Check archived canoncial to find alias
        canonical_alias = self._check_archive_canonical(wayback_url, html)
        tracer.debug(f'_check_archive_canonical: {canonical_alias}')
        if canonical_alias:
            return canonical_alias, {'type': 'archive_canonical', 'value': 'N/A'}

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


        tracer.debug(f'backlink guess total: {guess_total}')
        tracer.debug(f'backlink outgoing queue: {outgoing_queue}')
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
                r_dict = self.discover_backlinks(src, url, title, content, html, url_ts)
                # print(r_dict)
                status, reason = r_dict['status'], r_dict['reason']
                tracer.discover(url, src, r_dict.get("wayback_src"), status, reason, r_dict.get('archive'), r_dict.get("live"))
                # if early_exit(status, reason):
                #     break
                if status == 'found':
                    return r_dict['url(s)'], {'type': reason[0], 'value': reason[1]}
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
            
            # *Trim low-rank urls, and do deduplications
            outgoing_queue.sort(key=lambda x: x[2])
            dedup,uniq_q, uniq_c = set(), [], 0
            while len(dedup) < trim_size and uniq_c < len(outgoing_queue):
                if outgoing_queue[uniq_c][0] not in dedup:
                    dedup.add(outgoing_queue[uniq_c][0])
                    uniq_q.append(outgoing_queue[uniq_c])
                uniq_c += 1
            outgoing_queue = uniq_q

        return None, {}
        