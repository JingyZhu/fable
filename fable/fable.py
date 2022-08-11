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
        self.histredirer = histredirector.HistRedirector(memo=self.memo,  proxies=proxies)
        self.searcher = searcher.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
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

    def infer_on_example(self, url, meta, alias):
        """
        Called whenever search/discover found new aliases
        Return: [URLs found aliases through inference]
        """
        new_finds = []
        self.inferer.add_url_alias(url, meta, alias)
        example = (url, meta, alias)
        found_aliases = self.inferer.infer_on_example(example)
        any_added = False
        for infer_url, (infer_alias, reason) in found_aliases.items():
            reorg_title = self.db.reorg.find_one({'url': infer_url})
            title = reorg_title['title'] if 'title' in reorg_title and isinstance(reorg_title['title'], str) else ''
            update_dict = {"reorg_url": infer_alias, "by": {"method": "infer"}}
            update_dict['by'].update(reason)
            self.db.reorg.update_one({'url': infer_url}, {'$set': {self.classname:update_dict}})
            added = self.inferer.add_url_alias(infer_url, (title,), infer_alias)
            any_added = any_added or added
            new_finds.append(infer_url)
        # if not any_added: 
        #     break
        # examples = success
        # found_aliases = self.inferer.infer_new(examples)
        return new_finds

    def infer(self):
        """TODO: What needs to be logged?"""
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

    def hist_redir(self, required_urls):
        """
        Required urls: URLs that will be run on
        """
        if self.similar.site is None or self.site not in self.similar.site:
            self.similar.clear_titles()
            if not self.similar._init_titles(self.site):
                self.tracer.warn(f"Similar._init_titles: Fail to get homepage of {self.site}")
                return []

        reorg_checked = list(self.db.reorg.find({"hostname": self.site, self.classname: {"$exists": True}}))
        reorg_checked = set([u['url'] for u in reorg_checked])
        broken_urls = set([ru for ru in required_urls if ru not in reorg_checked])

        self.tracer.info(f'Discover SITE: {self.site} #URLS: {len(broken_urls)}')
        found = []
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.tracer.info(f'URL: {i} {url}')
            method = 'wayback_alias'
            self.tracer.info("Start wayback alias")
            start = time.time()
            discovered = self.histredirer.wayback_alias(url, require_neighbor=True, homepage_redir=False)
            if discovered:
                trace = {'type': 'wayback_alias', 'value': None}

            end = time.time()
            self.tracer.info(f'Runtime (historical redirection): {end - start}')
            self.tracer.flush()
            update_dict = {}
            has_title = self.db.reorg.find_one({'url': url})
            if has_title is None:
                has_title = {'url': url, 'hostname': self.site}
                self.db.reorg.update_one({'url': url}, {'$set': has_title}, upsert=True)
            # * Get title of the URL (if available)
            if 'title' not in has_title:
                try:
                    wayback_url = self.memo.wayback_index(url)
                    html = self.memo.crawl(wayback_url)
                    title = self.memo.extract_title(html, version='mine')
                except: # No snapthost on wayback
                    self.tracer.error(f'WB_Error {url}: Fail to get data from wayback')
                    title = 'N/A'
            else:
                title = has_title['title']

            if discovered is not None:
                self.tracer.info(f'Found reorg: {discovered}')
                found.append(url)
                update_dict.update({'reorg_url': discovered, 'by':{
                    "method": method
                }})
                by_discover = {k: v for k, v in trace.items() if k not in ['trace', 'backpath']}
                update_dict['by'].update(by_discover)

            # * Update dict correspondingly
            try:
                self.db.reorg.update_one({'url': url}, {'$set': {self.classname: update_dict, 'title': title}})
            except Exception as e:
                self.tracer.warn(f'Discover update DB: {str(e)}')
        return found


    def search(self, url, nocompare=True, fuzzy=True):
        """
        Search for a single URL
        nocompare: True: run search_nocompare, False: search
        fuzzy: if nocompare=False, fuzzy argument for search

        Return: [ [url, [title,], alias, reason] ]
        """
        site = he.extract(url)
        if self.similar.site is None or site not in self.similar.site:
            self.similar._init_titles(site)
        
        # * Get title
        title = ''
        try:
            print(url)
            wayback_url = self.memo.wayback_index(url)
            if wayback_url:
                trials += 1
                wayback_html = self.memo.crawl(wayback_url)
                title = self.memo.extract_title(wayback_html)
        except: pass

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
        search_aliases = []
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