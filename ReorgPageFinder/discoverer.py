"""
Discover backlinks to today's page
"""
import os
from urllib.parse import urlsplit, urlparse, parse_qsl, urlunsplit
from itertools import chain, combinations
from bs4 import BeautifulSoup
from queue import Queue

import sys
sys.path.append('../')
import config
from utils import search, crawl, text_utils, url_utils, sic_transit

BUDGET = 16
GUESS = 5
OUTGOING = 3

class Discoverer:
    def __init__(self, depth=3, corpus=[], proxies={}):
        self.depth = depth
        self.corpus = corpus
        self.PS = crawl.ProxySelector(proxies)
        self.wayback = {} # {url: (wayback ts, html)
        self.liveweb = {} # {url: html}
        self.budget = BUDGET
    
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
    
    def link_same_page(self, html, backlined_url, backlinked_html):
        """
        See whether backedlinked_html contains links to the same page as html
        html: html file of the original url want to find copy
        backlinked_html: html which could be linking to the html

        Returns: (link, link's html) which is a copy of html if exists. None otherwise
        """
        outgoing_links = crawl.outgoing_links(backlinked_url, backlinked_url, wayback=False)
        outgoing_htmls = {}
        for outgoing_link = in outgoing_links:
            outgoing_htmls[outgoing_link] = crawl.requests_crawl(outgoing_url, proxies=self.PS.select())
        top_similar = similar.similar(html, outgoing_htmls)
        if top_similar[0] > 0.8: 
            return top_similar
        else:
            return None
    
    def find_same_link(self, wayback_sig, liveweb_html):
        """
        For same page from wayback and liveweb (still working). Find the same url from liveweb which matches to wayback
        
        Returns: If there is a match on the url, return url, Else: return None
        """
        pass

    def discover_backlinks(self, src, dst, dst_html):
        """
        For src and dst, see:
            1. If src is archived on wayback
            2. If src is linking to dst on wayback
            3. If src is still working today
        
        returns: (status, url(s)), status includes: found/looping/reorg/notfound
        """
        wayback_src = find_wayback_url(src)
        broken, reason = sic_transit.broken(src)
        if wayback_src is None: # No archive in wayback for guessed_url
            if broken:
                continue
            src_html = crawl.requests_crawl(src)
            top_similar = self.link_same_page(dst_html, src_url, src_html)
            if top_simiar is not None: 
                return "found", top_similar[0]
        else:
            wayback_src_html = crawl.requests_crawl(wayback_src[0])
            wayback_outgoing_sigs = crawl.outgoing_links_sig(wayback_src[0], wayback_src_html, wayback=True)
            wayback_linked = [False, '']
            for wayback_outgoing_link, anchor, sig in wayback_outgoing_sigs:
                if url_utils.url_match(wayback_outgoing_link, dst, wayback=True):
                    linked = [True, (wayback_outgoing_link, anchor, sig)]
                    break
            if wayback_linked[0] and not broken: # src linking to dst and is working today
                src_html = crawl.requests_crawl(src)
                matched_url = self.find_same_link(linked[1], src_html)
                if matched_url:
                    return "found", matched_url
                else:
                    top_similar = self.link_same_page(dst_html, src, src_html)
                    if top_simiar is not None: 
                        return "found", top_similar[0]
                    else: 
                        return "notfound", None
            elif not wayback_linked[0]: # Not linked to dst, need to look futher
                return "loop", wayback_outgoing_sigs
            else: # Linked to dst, but broken today
                return "reorg", wayback_outgoing_sigs 
    
    def discover(self, url, depth=self.depth):
        """
        Discover the potential reorganized site
        # TODO: 1. find_wayback_url implementation
                2. similar implementation
        """
        wayback_url = find_wayback_url(url)
        html = crawl.requests_crawl(wayback_url[0])
        guessed_urls = self.guess_backlinks(url)
        search_queue = Queue()
        for link in guessed_urls:
            search_queue.put((link, depth-GUESS))
        outgoing_links = crawl.outgoing_links(wayback_url, html, wayback=True)
        for link in outgoing_links:
            search_queue.put((url_utils.filter_wayback(link), depth-OUTGOING))
        
        while not search_queue.empty():
            src, depth = search_queue.get()
            reorg_url = self.discover_backlinks(src, dst, html)
