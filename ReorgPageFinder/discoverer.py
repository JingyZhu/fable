"""
Discover backlinks to today's page
"""
import os
from urllib.parse import urlsplit, urlparse, parse_qsl, urlunsplit
from itertools import chain, combinations
from bs4 import BeautifulSoup
from queue import Queue
from . import tools

import sys
sys.path.append('../')
import config
from utils import search, crawl, text_utils, url_utils, sic_transit

BUDGET = 16
GUESS = 5
OUTGOING = 3

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

    def link_same_page(self, content, backlinked_url, backlinked_html):
        """
        See whether backedlinked_html contains links to the same page as html
        content: content file of the original url want to find copy
        backlinked_html: html which could be linking to the html

        Returns: (link, link's html) which is a copy of html if exists. None otherwise
        """
        outgoing_links = crawl.outgoing_links(backlinked_url, backlinked_html, wayback=False)
        outgoing_contents = {}
        for outgoing_link in outgoing_links:
            html = self.memo.crawl(outgoing_link, proxies=self.PS.select())
            if html is None: continue
            print(outgoing_link)
            outgoing_content = text_utils.extract_body(html, version='domdistiller')
            similars = self.similar.search_similar(content, {outgoing_link: outgoing_content})
            if len(similars) > 0:
                return similars[0]
        return
    
    def find_same_link(self, wayback_sig, liveweb_url, liveweb_html):
        """
        For same page from wayback and liveweb (still working). Find the same url from liveweb which matches to wayback
        
        Returns: If there is a match on the url, return url, Else: return None
        """
        live_outgoing_sigs = crawl.outgoing_links_sig(liveweb_url, liveweb_html)
        matched_url = self.similar.match_url_sig(wayback_sig, live_outgoing_sigs)
        return matched_url

    def discover_backlinks(self, src, dst, dst_html):
        """
        For src and dst, see:
            1. If src is archived on wayback
            2. If src is linking to dst on wayback
            3. If src is still working today
        
        returns: (status, url(s)), status includes: found/loop/reorg/notfound
        """
        print(src, dst)
        wayback_src = self.memo.wayback_index(src)
        broken, reason = sic_transit.broken(src)
        dst_content = text_utils.extract_body(dst_html, version='domdistiller')
        if wayback_src is None: # No archive in wayback for guessed_url
            if broken:
                return "not found", None
            src_html, src = self.memo.crawl(src, final_url=True)
            top_similar = self.link_same_page(dst_content, src, src_html)
            if top_similar is not None: 
                return "found", top_similar[0]
        else:
            wayback_src_html = self.memo.crawl(wayback_src[0])
            wayback_outgoing_sigs = crawl.outgoing_links_sig(wayback_src[0], wayback_src_html, wayback=True)
            wayback_linked = [False, '']
            for wayback_outgoing_link, anchor, sibtext in wayback_outgoing_sigs:
                if url_utils.url_match(wayback_outgoing_link, dst, wayback=True):
                    # TODO: linked can be multiple links
                    linked = [True, (wayback_outgoing_link, anchor, sibtext)]
                    break
            if wayback_linked[0] and not broken: # src linking to dst and is working today
                src_html = self.memo.crawl(src)
                matched_url = self.find_same_link(linked[1], src, src_html)
                if matched_url:
                    return "found", matched_url
                else:
                    top_similar = self.link_same_page(dst_content, src, src_html)
                    if top_similar is not None: 
                        return "found", top_similar[0]
                    else: 
                        return "notfound", None
            elif not wayback_linked[0]: # Not linked to dst, need to look futher
                return "loop", wayback_outgoing_sigs
            else: # Linked to dst, but broken today
                return "reorg", wayback_outgoing_sigs 
    
    def discover(self, url, depth=None):
        """
        Discover the potential reorganized site
        # TODO: 1. similar implementation
        """
        if depth is None: depth = self.depth
        wayback_url = self.memo.wayback_index(url)
        html, wayback_url = self.memo.crawl(wayback_url, final_url=True)
        guessed_urls = self.guess_backlinks(url)
        search_queue = Queue()
        seen = set()
        for link in guessed_urls:
            if link not in seen:
                search_queue.put((link, depth-GUESS))
                seen.add(link)
        outgoing_links = crawl.outgoing_links(wayback_url, html, wayback=True)
        for link in outgoing_links:
            if link not in seen:
                search_queue.put((url_utils.filter_wayback(link), depth-OUTGOING))
                seen.add(link)

        while not search_queue.empty():
            src, depth = search_queue.get()
            print(f"got: {src} {depth}")
            status, msg_urls = self.discover_backlinks(src, url, html)
            if status == 'found':
                return msg_urls
            elif status == 'loop':
                if depth >= GUESS:
                    guessed_urls = self.guess_backlinks(src)
                    for link in guessed_urls:
                        if link not in seen:
                            search_queue.put((link, depth-GUESS))
                            seen.add(link)
                if depth >= OUTGOING:
                    for outlink, _, _ in msg_urls:
                        if outlink not in seen:
                            search_queue.put((url_utils.filter_wayback(outlink), depth - OUTGOING))
                            seen.add(outlink)
            elif status == 'reorg':
                reorg_src = self.discover(src, depth)
                if reorg_src is not None and reorg_src not in seen:
                    search_queue.put((reorg_src, depth))
                    seen.add(reorg_src)
        return

