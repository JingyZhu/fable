"""
Discover backlinks to today's page
"""
import os
from urllib.parse import urlsplit, urlparse, parse_qsl, urlunsplit
from itertools import chain, combinations
from bs4 import BeautifulSoup
from queue import Queue
from . import tools
from collections import defaultdict

import sys
sys.path.append('../')
import config
from utils import search, crawl, text_utils, url_utils, sic_transit

import logging
logger = logging.getLogger('logger')

BUDGET = 11
GUESS = 3
OUTGOING = 4
CUT = 30

def wsum_simi(simi):
    return 2/3*simi[0] + 1/3*simi[1]
class Discoverer:
    def __init__(self, depth=BUDGET, corpus=[], proxies={}, memo=None, similar=None):
        self.depth = depth
        self.corpus = corpus
        self.PS = crawl.ProxySelector(proxies)
        self.wayback = {} # {url: wayback ts}
        self.crawled = {} # {url: html}
        self.budget = BUDGET
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar() 
    
    def guess_backlinks(self, url):
        """
        Guess backlinks by returning:
            The parent url & If url with query, no query / partial query
        """
        def powerset(iterable):
            "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
            s = list(iterable)
            return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))
        us = urlsplit(url)
        path, query = us.path, us.query
        guessed_urls = []
        path_dir = os.path.dirname(path)
        if path != path_dir: # Not root dir
            us_tmp = us._replace(path=path_dir, query='')
            guessed_urls.append(urlunsplit(us_tmp))
        if not query:
            return guessed_urls
        qsl = parse_qsl(query)
        if len(qsl) == 0:
            us_tmp = us._replace(query='')
            guessed_urls.append(urlunsplit(us_tmp))
            return guessed_urls
        for sub_q in powerset(qsl):
            if len(sub_q) == len(qsl): continue
            us_tmp = us._replace(query='&'.join([f'{kv[0]}={kv[1]}' for kv in sub_q]))
            guessed_urls.append(urlunsplit(us_tmp))
        return guessed_urls

    def link_same_page(self, dst, title, content, backlinked_url, backlinked_html, cut=CUT):
        """
        See whether backedlinked_html contains links to the same page as html
        content: content file of the original url want to find copy
        backlinked_html: html which could be linking to the html
        cut: Max number of outlinks to test on. If set to <=0, there is no limit

        Returns: (link, link's html) which is a copy of html if exists. None otherwise
        """
        backlinked_content = self.memo.extract_content(backlinked_html, version='domdistiller')
        backlinked_title = self.memo.extract_title(backlinked_html, version='domdistiller')
        similars = self.similar.similar(dst, title, content, {backlinked_url: backlinked_title}, {backlinked_url: backlinked_content})
        if len(similars) > 0:
            return similars[0]

        # outgoing_links = crawl.outgoing_links(backlinked_url, backlinked_html, wayback=False)
        he = url_utils.HostExtractor()
        outgoing_sigs = crawl.outgoing_links_sig(backlinked_url, backlinked_html, wayback=False)
        outgoing_sigs = [osig for osig in outgoing_sigs if he.extract(osig[0]) == he.extract(dst)]
        if cut <= 0:
            cut = len(outgoing_sigs)
        if len(outgoing_sigs) > cut:
            repr_text = content if content != '' else title
            self.similar.tfidf._clear_workingset()
            scoreboard = defaultdict(lambda: (0, 0))
            c = [s[1] for s in outgoing_sigs] + [' '.join(s[2]) for s in outgoing_sigs] + [repr_text]
            self.similar.tfidf.add_corpus(c)
            for outlink, anchor, sig in outgoing_sigs:
                simis = (self.similar.tfidf.similar(anchor, repr_text), self.similar.tfidf.similar(' '.join(sig), repr_text))
                scoreboard[outlink] = max(scoreboard[outlink], simis, key=lambda x: wsum_simi(x))
            scoreboard = sorted(scoreboard.items(), key=lambda x: wsum_simi(x[1]), reverse=True)
            outgoing_links = [sb[0] for sb in scoreboard[:cut]]
        else:
            outgoing_links = [osig[0] for osig in outgoing_sigs]

        # outgoing_contents = {}
        for outgoing_link in outgoing_links:
            if he.extract(dst) != he.extract(outgoing_link):
                continue
            html = self.memo.crawl(outgoing_link, proxies=self.PS.select())
            if html is None: continue
            logger.info(f'Test if outgoing link same: {outgoing_link}')
            outgoing_content = self.memo.extract_content(html, version='domdistiller')
            outgoing_title = self.memo.extract_title(html, version='domdistiller')
            similars = self.similar.similar(dst, title, content, {outgoing_link: outgoing_title}, {outgoing_link: outgoing_content})
            if len(similars) > 0:
                return similars[0]
        return
    
    def find_same_link(self, wayback_sigs, liveweb_url, liveweb_html):
        """
        For same page from wayback and liveweb (still working). Find the same url from liveweb which matches to wayback
        
        Returns: If there is a match on the url, return url, Else: return None
        """
        live_outgoing_sigs = crawl.outgoing_links_sig(liveweb_url, liveweb_html)
        for wayback_sig in wayback_sigs:
            matched_url = self.similar.match_url_sig(wayback_sig, live_outgoing_sigs)
            if matched_url is not None:
                return matched_url
        return
    
    def loop_cand(self, url, outgoing_url):
        """
        See whether outgoing_url is worth looping through
        Creteria: 1. Same domain 2. No deeper than url
        """
        he = url_utils.HostExtractor()
        if 'web.archive.org' in outgoing_url:
            outgoing_url = url_utils.filter_wayback(outgoing_url)
        if he.extract(url) != he.extract(outgoing_url):
            return False
        if urlsplit(url).path in urlsplit(outgoing_url).path:
            return False
        return True

    def discover_backlinks(self, src, dst, dst_title, dst_content, dst_html):
        """
        For src and dst, see:
            1. If src is archived on wayback
            2. If src is linking to dst on wayback
            3. If src is still working today
        
        returns: (status, url(s)), status includes: found/loop/reorg/notfound
        """
        logger.info(f'Backlinks: {src} {dst}')
        wayback_src = self.memo.wayback_index(src)
        broken, reason = sic_transit.broken(src, html=True)
        if wayback_src is None: # No archive in wayback for guessed_url
            if broken:
                logger.info(f'Discover backlinks broken: {reason}')
                return "notfound", None
            src_html, src = self.memo.crawl(src, final_url=True, max_retry=5)
            top_similar = self.link_same_page(dst, dst_title, dst_content, src, src_html)
            if top_similar is not None: 
                return "found", top_similar[0]
            else:
                return "notfound", None
        else:
            wayback_src_html = self.memo.crawl(wayback_src)
            wayback_outgoing_sigs = crawl.outgoing_links_sig(wayback_src, wayback_src_html, wayback=True)
            wayback_linked = [False, []]
            for wayback_outgoing_link, anchor, sibtext in wayback_outgoing_sigs:
                if url_utils.url_match(wayback_outgoing_link, dst, wayback=True):
                    # TODO: linked can be multiple links
                    wayback_linked[0] = True
                    wayback_linked[1].append((wayback_outgoing_link, anchor, sibtext))
            logger.info(f'Wayback linked: {wayback_linked[1]}')
            if wayback_linked[0] and not broken: # src linking to dst and is working today
                src_html, src = self.memo.crawl(src, final_url=True)
                matched_url = self.find_same_link(wayback_linked[1], src, src_html)
                if matched_url:
                    return "found", matched_url[0]
                else:
                    top_similar = self.link_same_page(dst, dst_title, dst_content, src, src_html)
                    if top_similar is not None: 
                        return "found", top_similar[0]
                    else: 
                        return "notfound", None
            elif not wayback_linked[0]: # Not linked to dst, need to look futher
                return "loop", wayback_outgoing_sigs
            else: # Linked to dst, but broken today
                return "reorg", wayback_outgoing_sigs 
    
    def discover(self, url, depth=None, seen=None, trim_size=10):
        """
        Discover the potential reorganized site
        Trim size: The largest size of outgoing queue
        # TODO: 1. similar implementation
        """
        if depth is None: depth = self.depth
        try:
            wayback_url = self.memo.wayback_index(url)
            html, wayback_url = self.memo.crawl(wayback_url, final_url=True)
            content = self.memo.extract_content(html, version='domdistiller')
            title = self.memo.extract_title(html, version='domdistiller')
        except Exception as e:
            logger.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
            return
        repr_text = content if content != '' else title
        guessed_urls = self.guess_backlinks(url)
        guess_queue = [(g, depth - GUESS) for g in guessed_urls]
        guess_total = defaultdict(int, {g: depth-GUESS for g in guessed_urls})
        seen = set() if seen is None else seen
        # seen.update(guessed_urls)

        # Add guessed links
        while len(guess_queue) > 0:
            link, link_depth = guess_queue.pop(0)
            if link_depth >= GUESS:
                for guessed_url in self.guess_backlinks(link):
                    guess_queue.append((guessed_url, link_depth-GUESS))
                    guess_total[guessed_url] = max(guess_total[guessed_url], link_depth-GUESS)
                    # seen.add(guessed_url)
        guess_total = list(guess_total.items())

        outgoing_queue = []
        outgoing_sigs = crawl.outgoing_links_sig(wayback_url, html, wayback=True)
        self.similar.tfidf._clear_workingset()
        scoreboard = defaultdict(lambda: (0, 0))
        c = [s[1] for s in outgoing_sigs] + [' '.join(s[2]) for s in outgoing_sigs] + [repr_text]
        self.similar.tfidf.add_corpus(c)
        for outlink, anchor, sig in outgoing_sigs:
            outlink = url_utils.filter_wayback(outlink)
            # For each link, find highest link score
            if outlink not in seen and self.loop_cand(url, outlink):
                simis = (self.similar.tfidf.similar(anchor, repr_text), self.similar.tfidf.similar(' '.join(sig), repr_text))
                scoreboard[outlink] = max(scoreboard[outlink], simis, key=lambda x: wsum_simi(x))
        for outlink, simis in scoreboard.items():
            outgoing_queue.append((outlink, depth-OUTGOING, simis))

        outgoing_queue.sort(reverse=True, key=lambda x: wsum_simi(x[2]))
        outgoing_queue = outgoing_queue[: trim_size+1] if len(outgoing_queue) >= trim_size else outgoing_queue
        
        while len(guess_total) + len(outgoing_queue) > 0:
            # Ops for guessed links
            two_src = []
            if len(guess_total) > 0:
                two_src.append(guess_total.pop(0))
            if len(outgoing_queue) > 0:
                two_src.append(outgoing_queue.pop(0)[:2])
            for item in two_src:
                src, link_depth = item
                logger.info(f"Got: {src} depth:{link_depth} guess_total:{len(guess_total)} outgoing_queue:{len(outgoing_queue)}")
                seen.add(src)
                status, msg_urls = self.discover_backlinks(src, url, title, content, html)
                logger.info(status)
                if status == 'found':
                    return msg_urls
                elif status == 'loop':
                    if link_depth >= OUTGOING:
                        c = [s[1] for s in msg_urls] + [' '.join(s[2]) for s in msg_urls] + [repr_text]
                        self.similar.tfidf.add_corpus(c)
                        scoreboard = defaultdict(lambda: (0, 0))
                        for outlink, anchor, sig in msg_urls:
                            outlink = url_utils.filter_wayback(outlink)
                            # For each link, find highest link score
                            if outlink not in seen and self.loop_cand(url, outlink):
                                simis = (self.similar.tfidf.similar(anchor, repr_text), self.similar.tfidf.similar(' '.join(sig), repr_text))
                                scoreboard[outlink] = max(scoreboard[outlink], simis, key=lambda x: wsum_simi(x))
                        for outlink, simis in scoreboard.items():
                            outgoing_queue.append((outlink, link_depth-OUTGOING, simis))
            
            outgoing_queue.sort(reverse=True, key=lambda x: wsum_simi(x[2]))
            outgoing_queue = outgoing_queue[:trim_size+1] if len(outgoing_queue) > trim_size else outgoing_queue
            # elif status == 'reorg':
            #     reorg_src = self.discover(src, depth=depth, seen=seen)
            #     if reorg_src is not None and reorg_src not in seen:
            #         search_queue.put((reorg_src, depth))
            #         seen.add(reorg_src)
        return

