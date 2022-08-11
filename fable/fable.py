from fable import histredirector, searcher, inferer, verifier, tools
from urllib.parse import urlsplit, parse_qsl
import os
import time
import logging

from . import config
from .tracer import tracer as tracing
from .utils import url_utils, crawl, sic_transit

db = config.DB
he = url_utils.HostExtractor()


class AliasFinder:
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, tracer=None,\
                classname='fable', logname=None, loglevel=logging.INFO):
        """
        memo: tools.Memoizer class for access cached crawls & API calls. If None, initialize one.
        similar: tools.Similar class for similarity matching. If None, initialize one.
        tracer: self-extended logger
        classname: Class (key) that the db will update data in the corresponding document
        logname: The log file name that will be output msg. If not specified, use classname
        """
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.PS = crawl.ProxySelector(proxies)
        self.histredirector = histredirector.HistRedirector(memo=self.memo,  proxies=proxies)
        self.searcher = searcher.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.verifier = verifier.Verifier(fuzzy=1)
        self.db = db
        self.site = None
        self.url_title = {}
        self.classname = classname
        self.logname = classname if logname is None else logname
        self.tracer = tracer if tracer is not None else self._init_tracer(loglevel=loglevel)

    def _init_tracer(self, loglevel):
        logging.setLoggerClass(tracing)
        tracer = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tracer._set_meta(self.classname, logname=self.logname, db=self.db, loglevel=loglevel)
        return tracer
    

    def init_site(self, site, urls=[]):
        self.site = site
        # * Initialized tracer
        if len(self.tracer.handlers) > 2:
            self.tracer.handlers.pop()
        formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
        if not os.path.exists('logs'):
            os.mkdir('logs')
        file_handler = logging.FileHandler(f'./logs/{site}.log')
        file_handler.setFormatter(formatter)
        self.tracer.addHandler(file_handler)

    def clear_site(self):
        self.site = None
        self.pattern_dict = None
        self.seen_reorg_pairs = None
        self.tracer.handlers.pop()
    
    def _get_title(self, url):
        if url in self.url_title:
            return self.url_title[url]
        wayback_url = self.memo.wayback_index(url)
        if wayback_url:
            wayback_html = self.memo.crawl(wayback_url)
            title = self.memo.extract_title(wayback_html)
        else:
            title = ""
        self.url_title[url] = title
        return title

    def infer(self, urls, verified_cands):
        """
        urls: URLs to infer

        """
        if self.similar.site is None or self.site not in self.similar.site:
            self.similar.clear_titles()
            if not self.similar._init_titles(self.site):
                self.tracer.warn(f"Similar._init_titles: Fail to get homepage of {self.site}")
                return
        found_aliases = self.inferer.infer_all()
        any_added = False
        for infer_url, (infer_alias, reason) in found_aliases.items():
            reorg_title = self.db.reorg.find_one({'url': infer_url})
            title = reorg_title['title'] if 'title' in reorg_title and isinstance(reorg_title['title'], str) else ''
            update_dict = {"reorg_url": infer_alias, "by": {"method": "infer"}}
            update_dict['by'].update(reason)
            self.db.reorg.update_one({'url': infer_url}, {'$set': {self.classname: update_dict}})
            added = self.inferer.add_url_alias(infer_url, (title,), infer_alias)
            any_added = any_added or added
        self.tracer.flush()

    def hist_redir(self, urls):
        """
        Return: [ [url, [title,], [aliases (w/ history)], reason] ]
        """
        hist_aliases = []
        aliases = self.histredirector.wayback_alias_batch_history(urls)

        for url, r in aliases.items():
            title = self._get_title(url)
            reason = {}
            if r:
                reason = {"method": "wayback_alias", "type": "wayback_alias"}
                hist_aliases.append([
                    url,
                    [title],
                    r,
                    reason
                ])
        return hist_aliases

    def search(self, urls, nocompare=True, fuzzy=True):
        """
        Search for a set of similar URLs (similar URLs: URLs under the same directory)
        nocompare: True: run search_nocompare, False: search
        fuzzy: if nocompare=False, fuzzy argument for search

        Return: [ [url, [title,], alias, reason] ]
        """
        if isinstance(urls, str): urls = [urls]
        first_url = urls[0]
        site = he.extract(first_url)
        if self.similar.site is None or site not in self.similar.site:
            self.similar._init_titles(site)
        
        search_aliases = []
        for url in urls:
            title = self._get_title(url)
            
            # * Search
            if nocompare:
                aliases = self.searcher.search_nocompare(url, search_engine='bing')
                aliases += self.searcher.search_nocompare(url, search_engine='google')
                aliases = {a[0]: a for a in reversed(aliases)}
                aliases = list(aliases.values())
            else:
                aliases = self.searcher.search(url, search_engine='bing', fuzzy=fuzzy)
                if aliases[0] is None:
                    aliases = self.searcher.search(url, search_engine='google', fuzzy=fuzzy)
            
            # * Merge results
            seen = set()
            if len(aliases) > 0 and aliases[0]:
                for a in aliases:
                    reason = a[1]
                    seen.add(a[0])
                    search_aliases.append([url, [title,], a[0], reason])

            all_search = self.searcher.search_results(url)
            for ase in all_search:
                if ase in seen: continue
                seen.add(ase)
                search_aliases.append([url, [title,], ase, {'method': 'search', 'type': 'fuzzy_search'}])
        return search_aliases
    
    def verify(self, urls, candidates):
        """
        Verify the candidates found for urls
        candidates: [ [url, [title,], alias, reason] ]

        Return: verified [ [url, [title,], alias, reason] ]
        """
        # * Form candidates for verifier
        netloc = url_utils.netloc_dir(urls[0], exclude_index=True)
        cand_obj = {'netloc_dir': netloc, 'alias': [], 'examples': []}
        for cand in candidates:
            if cand[0] in urls:
                cand_obj['alias'].append(cand)
        
        # * Verify candidates for aliases
        aliases = []
        self.verifier.add_aliasexample(cand_obj, clear=True)
        for url in urls:
            alias = self.verifier.verify_url(url)
            title = self._get_title(url)
            for a, r in alias:
                aliases.append([url, [title,], a, r])
        return aliases