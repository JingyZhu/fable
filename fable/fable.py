from fable import discoverer, searcher, inferer, tools
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit, parse_qsl
import os
from collections import defaultdict
import time
import json
import logging

from . import config
from .tracer import tracer as tracing
from .utils import text_utils, url_utils, crawl, sic_transit

db = config.DB
he = url_utils.HostExtractor()


def unpack_ex(ex):
    (url, title), reorg = ex
    return url, title, reorg

class ReorgPageFinder:
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, tracer=None, logname='fable', loglevel=logging.INFO):
        """
        tracer: self-extended logger
        """
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.PS = crawl.ProxySelector(proxies)
        self.searcher = searcher.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.discoverer = discoverer.Discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
        self.pattern_dict = None
        self.seen_reorg_pairs = None
        self.logname = logname
        self.tracer = tracer if tracer is not None else self._init_tracer(loglevel=loglevel)

    def _init_tracer(self, loglevel):
        logging.setLoggerClass(tracing)
        tracer = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tracer._set_meta(self.logname, self.db, loglevel)
        return tracer
    
    def init_site(self, site, urls):
        self.site = site
        objs = []
        already_in = list(self.db.reorg.find({'hostname': site}))
        already_in = set([u['url'] for u in already_in])
        for url in urls:
            if url in already_in:
                continue
            objs.append({'url': url, 'hostname': site})
            # TODO May need avoid insert false positive 
            if not sic_transit.broken(url):
                try:
                    self.db.na_urls.update_one({'_id': url}, {'$set': 
                        {'url': url, 'hostname': site, 'url': url, 'false_positive': True}}, upsert=True)
                except: pass
        try:
            self.db.reorg.insert_many(objs, ordered=False)
        except: pass
        reorg_keys = {'reorg_url', 'reorg_url_search', 'reorg_url_discover_test', 'reorg_url_discover', 'reorg_url_infer'}
        reorg_urls = self.db.reorg.find({'hostname': site, self.logname: {'$exists': True}})
        # ? Whether to infer on all classes, all only one? 
        # ? reorg_urls = [reorg for reorg in reorg_urls if len(set(reorg.keys()).intersection(reorg_keys)) > 0]
        self.pattern_dict = defaultdict(list)
        self.seen_reorg_pairs = set()
        for reorg_url in list(reorg_urls):
            # Patch the no title urls
            if 'title' not in reorg_url:
                wayback_reorg_url = self.memo.wayback_index(reorg_url['url'])
                reorg_html, wayback_reorg_url = self.memo.crawl(wayback_reorg_url, final_url=True)
                reorg_title = self.memo.extract_title(reorg_html, version='domdistiller')
                reorg_url['title'] = reorg_title
                self.db.reorg.update_one({'url': reorg_url['url']}, {'$set': {'title': reorg_title}})
            # ? for k in set(reorg_url.keys()).intersection(reorg_keys):
            # ?    self._add_url_to_patterns(reorg_url['url'], reorg_url['title'], reorg_url[k])
            self._add_url_to_patterns(reorg_url['url'], reorg_url['title'], reorg_url[self.logname]['reorg_url'])
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

    def _add_url_to_patterns(self, url, title, reorg):
        """
        Only applies to same domain currently
        Return bool on whether success
        """
        # if he.extract(reorg) != he.extract(url):
        #     return False
        patterns = url_utils.gen_path_pattern(url)
        if (url, reorg) in self.seen_reorg_pairs:
            return True
        else:
            self.seen_reorg_pairs.add((url, reorg))
        if len(patterns) <= 0: return False
        for pat in patterns:
            self.pattern_dict[pat].append(((url, title), reorg))
        return True

    def _most_common_output(self, examples):
        """
        Given a list of examples, return ones with highest # common pattern

        Return: List of examples in highest common pattern
        """
        output_patterns = defaultdict(list)
        for ex in examples:
            reorg_url = ex[1]
            reorg_pats = url_utils.gen_path_pattern(reorg_url)
            for reorg_pat in reorg_pats:
                output_patterns[reorg_pat].append(ex)
            output_patterns = sorted(output_patterns.items(), key=lambda x:len(x[1]), reverse=True)
            output_pattern, output_ex = output_patterns[0]
            print(output_pattern, len(output_ex))
            return output_ex

    def query_inferer(self, examples):
        """
        examples: Lists of (url, title), reorg_url. 
                Should already be inserted into self.pattern_dict
        """
        if len(examples) <= 0:
            return []
        patterns = set() # All patterns in example
        for (url, title), reorg_url in examples:
            pats = url_utils.gen_path_pattern(url)
            patterns.update(pats)
        patterns = list(patterns)
        broken_urls = self.db.reorg.find({'hostname': self.site})
        # infer
        reorg_keys = {'by', 'by_infer', 'by_search', 'by_discover', 'by_discover_test'}
        broken_urls = [reorg for reorg in broken_urls if len(set(reorg.keys()).intersection(reorg_keys)) == 0]
        self.db.reorg.update_many({'hostname': self.site, "title": ""}, {"$unset": {"title": ""}})
        infer_urls = defaultdict(list) # Pattern: [(urls, (meta))]
        for infer_url in list(broken_urls):
            for pat in patterns:
                if not url_utils.pattern_match(pat, infer_url['url']):
                    continue
                if 'title' not in infer_url:
                    try:
                        wayback_infer_url = self.memo.wayback_index(infer_url['url'])
                        wayback_infer_html = self.memo.crawl(wayback_infer_url)
                        title = self.memo.extract_title(wayback_infer_html)
                        self.db.reorg.update_one({'_id': infer_url['_id']}, {'$set': {'title': title}})
                    except Exception as e:
                        self.tracer.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
                        title = ""
                else: title = infer_url['title'] 
                infer_urls[pat].append((infer_url['url'], (title)))
        if len(infer_urls) <=0:
            return []
        success = []
        for pat, pat_urls in infer_urls.items():
            self.tracer.info(f'Pattern: {pat}')
            infered_dict_all = self.inferer.infer(self.pattern_dict[pat], pat_urls, site=self.site)
            common_output = self._most_common_output(self.pattern_dict[pat])
            # //print(common_output)
            infered_dict_common = self.inferer.infer(common_output, pat_urls, site=self.site)
            infered_dict = {url: list(set(infered_dict_all[url] + infered_dict_common[url])) for url in infered_dict_all}
            self.tracer.info(f'infered_dict: {json.dumps(infered_dict, indent=2)}')
            
            pat_infer_urls = {iu[0]: iu for iu in infer_urls[pat]} # url: pattern
            fp_urls = set([p[1] for p in self.pattern_dict[pat]])
            for infer_url, cand in infered_dict.items():
                # // logger.info(f'Infer url: {infer_url} {cand}')
                reorg_url, trace = self.inferer.if_reorg(infer_url, cand, compare=False, fp_urls=fp_urls)
                if reorg_url is not None:
                    self.tracer.info(f'Found by infer: {infer_url} --> {reorg_url}')
                    if not self.fp_check(infer_url, reorg_url):
                        by_dict = {'method': 'infer'}
                        by_dict.update(trace)
                        # Infer
                        self.db.reorg.update_one({'url': infer_url}, {'$set': {
                            self.logname: {
                                'reorg_url': reorg_url, 
                                'by': by_dict
                            }
                        }})
                        suc = ((pat_infer_urls[infer_url]), reorg_url)
                        self._add_url_to_patterns(*unpack_ex(suc))
                        success.append(suc)
                    else: # False positive
                        try: self.db.na_urls.update_one({'_id': infer_url}, {'$set': {
                                'false_positive_infer': True,
                                'hostname': self.site
                            }}, upsert=True)
                        except: pass
                self.db.checked.update_one({'_id': infer_url}, {'$set': {'infer': True}})
        return success

    def fp_check(self, url, reorg_url):
        """
        Determine False Positive

        returns: Boolean on if false positive
        """
        if url_utils.url_match(url, reorg_url):
            return True
        html, url = self.memo.crawl(url, final_url=True)
        reorg_html, reorg_url = self.memo.crawl(reorg_url, final_url=True)
        if html is None or reorg_html is None:
            return False
        content = self.memo.extract_content(html)
        reorg_content = self.memo.extract_content(reorg_html)
        self.similar.tfidf._clear_workingset()
        simi = self.similar.tfidf.similar(content, reorg_content)
        return simi >= 0.8

    def infer(self):
        urls = {}
        success = [None]
        for examples in self.pattern_dict.values():
            for example in examples:
                (url, _), _ = example
                urls[url] = example
        examples = list(urls.values())
        success = list(urls.values())
        while(len(success)) > 0:
            success = self.query_inferer(examples)
            for suc in success:
                self._add_url_to_patterns(*unpack_ex(suc))
            examples = success


    def search(self, infer=False, required_urls=None, title=True):
        """
        infer: Infer every time found a new alias
        Required urls: URLs that will be run on
        title: Whether title comparison is taken into consideration
        """
        if not title:
            self.similar.clear_titles()
        elif self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        # !_search
        noreorg_urls = list(self.db.reorg.find({"hostname": self.site, self.logname: {"$exists": False}}))
        searched_checked = self.db.checked.find({"hostname": self.site, "search": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        
        required_urls = set(required_urls) if required_urls else set([u['url'] for u in noreorg_urls])

        urls = [u for u in noreorg_urls if u['url'] not in searched_checked and u['url'] in required_urls]
        broken_urls = set([u['url'] for u in urls])
        self.tracer.info(f'Search SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.tracer.info(f'URL: {i} {url}')
            searched = self.searcher.search(url, search_engine='bing')
            if searched is None:
                searched = self.searcher.search(url, search_engine='google')
            update_dict = {}
            has_title = self.db.reorg.find_one({'url': url})
            # if has_title is None: # No longer in reorg (already deleted)
            #     continue
            if 'title' not in has_title or has_title['title'] == 'N/A':
                try:
                    wayback_url = self.memo.wayback_index(url)
                    html = self.memo.crawl(wayback_url)
                    title = self.memo.extract_title(html, version='domdistiller')
                except: # No snapthost on wayback
                    self.tracer.error(f'WB_Error {url}: Fail to get data from wayback')
                    try: self.db.na_urls.update_one({'_id': url}, {"$set": {
                        'url': url,
                        'hostname': self.site,
                        'no_snapshot': True
                    }}, upsert=True)
                    except: pass
                    continue
                update_dict = {'title': title}
            else:
                title = has_title['title']

            self.tracer.flush()

            if searched is not None:
                searched, trace = searched
                self.tracer.info(f"HIT: {searched}")
                fp = self.fp_check(url, searched)
                if not fp: # False positive test
                    # ! search
                    update_dict.update({'reorg_url': searched, 'by':{
                        "method": "search"
                    }})
                    update_dict['by'].update(trace)
                else:
                    try: self.db.na_urls.update_one({'_id': url}, {'$set': {
                            'url': url,
                            'false_positive_search': True, 
                            'hostname': self.site
                        }}, upsert=True)
                    except: pass
                    searched = None


            if len(update_dict) > 0:
                try:
                    self.db.reorg.update_one({'url': url}, {"$set": {self.logname: update_dict}} ) 
                except Exception as e:
                    self.tracer.warn(f'Second search update DB: {str(e)}')
            searched_checked.add(url)
            
            # ! TODO: temp
            # try:
            #     self.db.checked.update_one({'_id': url}, {"$set": {
            #         "url": url,
            #         "hostname": self.site,
            #         "search_2": True
            #     }}, upsert=True)
            # except: pass

            if not infer:
                continue

            if searched is not None:
                example = ((url, title), searched)
                added = self._add_url_to_patterns(*unpack_ex(example))
                if not added: 
                    continue
                success = self.query_inferer([example])
                while len(success) > 0:
                    added = False
                    for suc in success:
                        broken_urls.discard(unpack_ex(suc)[0])
                        a = self._add_url_to_patterns(*unpack_ex(suc))
                        added = added or a
                    if not added: 
                        break
                    examples = success
                    success = self.query_inferer(examples)
    
    def discover(self, infer=False, required_urls=None):
        """
        infer: Infer every time found a new alias
        Required urls: URLs that will be run on
        """
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        # ! discover
        noreorg_urls = list(self.db.reorg.find({"hostname": self.site, self.logname: {"$exists": False}}))
        discovered_checked = self.db.checked.find({"hostname": self.site, "discover": True})
        discovered_checked = set([sc['url'] for sc in discovered_checked])
        
        required_urls = set(required_urls) if required_urls else set([u['url'] for u in noreorg_urls])
        
        urls = [u for u in noreorg_urls if u['url'] not in discovered_checked and u['url'] in required_urls]
        broken_urls = set([bu['url'] for bu in urls])
        self.tracer.info(f'Discover SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.tracer.info(f'URL: {i} {url}')
            method, suffice = 'discover', False
            while True: # Dummy while lloop served as goto
                self.tracer.info("Start wayback alias")
                discovered = self.discoverer.wayback_alias(url)
                if discovered:
                    fp = self.fp_check(url, discovered)
                    if fp:
                        discovered = None
                    else:
                        trace = {'suffice': True, 'type': 'wayback_alias', 'value': None}
                        break
                
                self.tracer.info("Start backpath (latest)")
                discovered, trace = self.discoverer.bf_find(url, policy='latest')
                if discovered:
                    method = 'backpath_latest'
                    break
                
                self.tracer.info("Start discover")
                discovered, trace = self.discoverer.discover(url)
                if discovered:
                    break
                suffice = trace['suffice']

                break

            self.tracer.flush()
            update_dict = {}
            has_title = self.db.reorg.find_one({'url': url})
            # if has_title is None: # No longer in reorg (already deleted)
            #     continue
            if 'title' not in has_title:
                try:
                    wayback_url = self.memo.wayback_index(url)
                    html = self.memo.crawl(wayback_url)
                    title = self.memo.extract_title(html, version='domdistiller')
                except: # No snapthost on wayback
                    self.tracer.error(f'WB_Error {url}: Fail to get data from wayback')
                    try: self.db.na_urls.update_one({'_id': url}, {"$set": {
                        'url': url,
                        'hostname': self.site,
                        'no_snapshot': True
                    }}, upsert=True)
                    except: pass
                    title = 'N/A'
                update_dict = {'title': title}
            else:
                title = has_title['title']


            if discovered is not None:
                self.tracer.info(f'Found reorg: {discovered}')
                fp = self.fp_check(url, discovered)
                if not fp: # False positive test
                    # ! discover
                    update_dict.update({'reorg_url': discovered, 'by':{
                        "method": method
                    }})
                    by_discover = {k: v for k, v in trace.items() if k not in ['trace', 'backpath', 'suffice']}
                    # ! discover
                    update_dict['by'].update(by_discover)
                else:
                    # discover
                    try: self.db.na_urls.update_one({'_id': url}, {'$set': {
                            'url': url,
                            'false_positive_discover_test': True, 
                            'hostname': self.site
                        }}, upsert=True)
                    except: pass
                    discovered = None
            elif not suffice:
                try:
                    self.db.na_urls.update_one({'_id': url}, {'$set': {
                        'no_working_parent': True, 
                        'hostname': self.site
                    }}, upsert=True)
                except:pass


            if len(update_dict) > 0:
                try:
                    self.db.reorg.update_one({'url': url}, {'$set': {self.logname: update_dict}})
                except Exception as e:
                    self.tracer.warn(f'Discover update DB: {str(e)}')
            discovered_checked.add(url)
            # ! TODO: temp
            # try:
            #     self.db.checked.update_one({'_id': url}, {"$set": {
            #         "url": url,
            #         "hostname": self.site,
            #         "discover": True
            #     }}, upsert=True)
            # except Exception as e:
            #     self.tracer.warn(f'Discover update checked: {str(e)}')
            
            if not infer:
                continue

            # TEMP
            if discovered is not None:
                example = ((url, title), discovered)
                added = self._add_url_to_patterns(*unpack_ex(example))
                if not added:
                    continue
                success = self.query_inferer([example])
                while len(success) > 0:
                    added = False
                    for suc in success:
                        broken_urls.discard(unpack_ex(suc)[0])
                        a = self._add_url_to_patterns(*unpack_ex(suc))
                        added = added or a
                    if not added:
                        break
                    examples = success
                    success = self.query_inferer(examples)
