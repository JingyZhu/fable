"""
Discover backlinks to today's page
"""
import os
from urllib.parse import urlsplit, urlparse, parse_qsl, urlunsplit
from itertools import chain, combinations
import bs4

import sys
sys.path.append('../')
import config
from utils import search, crawl, text_utils, url_utils, sic_transit

class Discoverer:
    def __init__(self, depth=3, corpus=[], proxies={}):
        self.depth = depth
        self.corpus = corpus
        self.PS = crawl.ProxySelector(proxies)
        self.wayback = {} # {url: (wayback ts, html)
        self.liveweb = {} # {url: html}
    
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
    
    def find_same_link(self, wayback_html, wayback_sig, liveweb_html, liveweb_sig):
        """
        For same page from wayback and liveweb (still working). Find the same url from liveweb which matches to wayback
        """
        pass
    
    def discover(self, url, depth=self.depth):
        """
        Discover the potential reorganized site
        # TODO: 1. find_wayback_url implementation
                2. similar implementation
        """
        wayback_url = find_wayback_url(url)
        html = crawl.requests_crawl(wayback_url[0])
        guessed_urls = self.guess_backlinks(url)
        for guessed_url in guessed_urls:
            wayback_guessed_url = find_wayback_url(guessed_url)
            if wayback_guessed_url is None: # No archive in wayback for guessed_url
                broken, reason = sic_transit.broken(guessed_url)
                if broken:
                    continue
                guessed_html = crawl.requests_crawl(guessed_url)
                top_similar = self.link_same_page(html, guessed_url, guessed_html)
                if top_simiar is not None: 
                    return top_similar
            else:
                wayback_guessed_html = crawl.requests_crawl(wayback_guessed_url[0])
                wayback_outgoing_sigs = crawl.outgoing_links_sig(wayback_guessed_url[0], wayback_guessed_html, wayback=True)
                wayback_linked = [False, '']
                for wayback_outgoing_link, anchor, sig in wayback_outgoing_sigs:
                    if url_utils.url_match(url, wayback_outgoing_link):
                        linked = [True, (wayback_outgoing_link, anchor, sig)]
                        break
                if wayback_linked[0] and not sic_transit.broken(guessed_url)[0]: # Linked to url working
                    
                    guessed_html = crawl.requests_crawl(linked[1][0])

        outgoing_links = crawl.outgoing_links(wayback_url, html, wayback=True)
