from fable import discoverer, searcher, inferer, tools
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit, parse_qsl
import os
from collections import defaultdict
import time
import json
import logging

from fable import config
from fable.tracer import tracer as tracing
from fable.utils import text_utils, url_utils, crawl, sic_transit

db = config.DB
he = url_utils.HostExtractor()
SEARCH_CLASS = 'wiki_search'
DISCOVER_CLASS = 'wiki_discover'


def unpack_ex(ex):
    url, meta, reorg = ex
    return url, meta, reorg

class ReorgPageFinder:
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
        self.searcher = searcher.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.discoverer = discoverer.Discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
        self.pattern_dict = None
        self.seen_reorg_pairs = None
        self.classname = classname
        self.logname = classname if logname is None else logname
        self.tracer = tracer if tracer is not None else self._init_tracer(loglevel=loglevel)
        self.inference_classes = [self.classname] # * Classes looking on reorg for aliases sets
        self.aliases = set()

    def _init_tracer(self, loglevel):
        logging.setLoggerClass(tracing)
        tracer = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tracer._set_meta(self.classname, logname=self.logname, db=self.db, loglevel=loglevel)
        return tracer
    
    def set_inferencer_classes(self, classes):
        self.inference_classes = classes + [self.classname]

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
        site_reorg_urls = self.db.reorg.find({'hostname': site})
        # ? Whether to infer on all classes, all only one? 
        # ? reorg_urls = [reorg for reorg in reorg_urls if len(set(reorg.keys()).intersection(reorg_keys)) > 0]
        self.pattern_dict = defaultdict(list)
        self.seen_reorg_pairs = set()
        for reorg_url in list(site_reorg_urls):
            for iclass in self.inference_classes:
                if iclass in reorg_url:
                    self._add_url_to_patterns(reorg_url['url'], (reorg_url.get('title', ''),), reorg_url[iclass]['reorg_url'])
        
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
        self.aliases = set()

    # def _add_url_to_patterns(self, url, meta, reorg):
    #     """
    #     Only applies to same domain currently
    #     Return bool on whether success
    #     """
    #     # if he.extract(reorg) != he.extract(url):
    #     #     return False
    #     patterns = url_utils.gen_path_pattern(url)
    #     if (url, reorg) in self.seen_reorg_pairs:
    #         return True
    #     else:
    #         self.seen_reorg_pairs.add((url, reorg))
        
    #     if len(patterns) <= 0: return False
    #     if meta[0] == 'N/A':
    #         meta = list(meta)
    #         meta[0] = ''
    #         meta = tuple(meta)
    #     for pat in patterns:
    #         self.pattern_dict[pat].append((url, meta, reorg))
    #     return True

    def _add_url_to_patterns(self, url, meta, reorg):
        """
        Only applies to same domain currently
        Return bool on whether success
        """
        # if he.extract(reorg) != he.extract(url):
        #     return False
        netloc_dir = url_utils.netloc_dir(url)
        if (url, reorg) in self.seen_reorg_pairs:
            return True
        else:
            self.seen_reorg_pairs.add((url, reorg))
        
        # if len(patterns) <= 0: return False
        if meta[0] == 'N/A':
            meta = list(meta)
            meta[0] = ''
            meta = tuple(meta)
        # for pat in patterns:
        self.pattern_dict[netloc_dir].append((url, meta, reorg))
        return True

    # def _most_common_output(self, examples):
    #     """
    #     Given a list of examples, return ones with highest # common pattern

    #     Return: List of examples in highest common pattern
    #     """
    #     output_patterns = defaultdict(list)
    #     for ex in examples:
    #         reorg_url = ex[2]
    #         reorg_pats = url_utils.gen_path_pattern(reorg_url)
    #         for reorg_pat in reorg_pats:
    #             output_patterns[reorg_pat].append(ex)
    #     output_patterns = sorted(output_patterns.items(), key=lambda x:len(x[1]), reverse=True)
    #     output_pattern, output_ex = output_patterns[0]
    #     self.tracer.debug(f"_most_common_output: {output_pattern}, {len(output_ex)}")
    #     return output_ex

    def _most_common_output(self, examples):
        """
        Given a list of examples, return ones with highest # common pattern

        Return: List of examples in highest common pattern
        """
        output_patterns = defaultdict(list)
        for ex in examples:
            reorg_url = ex[2]
            netloc_dir = url_utils.netloc_dir(reorg_url)
            # for reorg_pat in reorg_pats:
            output_patterns[netloc_dir].append(ex)
        output_patterns = sorted(output_patterns.items(), key=lambda x:len(x[1]), reverse=True)
        output_pattern, output_ex = output_patterns[0]
        self.tracer.debug(f"_most_common_output: {output_pattern}, {len(output_ex)}")
        return output_ex

    def query_inferer(self, examples):
        """
        examples: Lists of (url, title), reorg_url. 
                Should already be inserted into self.pattern_dict
        
        returns: Returned successed (url, (meta)), reorg
        """
        if len(examples) <= 0:
            return []
        patterns = set() # All patterns in example
        for url, (title), reorg_url in examples:
             # ! Temp
            # pats = url_utils.gen_path_pattern(url)
            # patterns.update(pats)
             # ! End of Temp
            pat = url_utils.netloc_dir(url)
            patterns.add(pat)
        patterns = list(patterns)
        broken_urls = self.db.reorg.find({'hostname': self.site})
        # infer
        broken_urls = [reorg for reorg in broken_urls if \
            len(set(reorg.keys()).intersection(self.inference_classes)) == 0 \
            and reorg['url'] not in self.aliases]
        # self.db.reorg.update_many({'hostname': self.site, "title": ""}, {"$unset": {"title": ""}})
        infer_urls = defaultdict(list) # * {Pattern: [(urls, (meta,))]}
        for toinfer_url in list(broken_urls):
            for pat in patterns:
                # ! Temp
                # if not url_utils.pattern_match(pat, toinfer_url['url']):
                #     continue
                # ! End of temp
                if url_utils.netloc_dir(toinfer_url['url']) != pat:
                    continue

                # if 'title' not in infer_url:
                #     try:
                #         wayback_infer_url = self.memo.wayback_index(infer_url['url'])
                #         wayback_infer_html = self.memo.crawl(wayback_infer_url)
                #         title = self.memo.extract_title(wayback_infer_html)
                #         self.db.reorg.update_one({'_id': infer_url['_id']}, {'$set': {'title': title}})
                #     except Exception as e:
                #         self.tracer.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
                #         title = ""
                # else: title = infer_url['title'] 
                title = toinfer_url.get('title', '')
                if title == 'N/A': title = ''
                infer_urls[pat].append((toinfer_url['url'], (title,)))
        if len(infer_urls) <=0:
            return []
        success = []
        for pat, pat_urls in infer_urls.items():
            self.tracer.info(f'Pattern: {pat}')
            # * Do two inferences. One with all patterns, the other with most common output patterns
            infered_dict_all = self.inferer.infer(self.pattern_dict[pat], pat_urls, site=self.site)
            common_output = self._most_common_output(self.pattern_dict[pat])# //print(common_output)
            infered_dict_common = self.inferer.infer(common_output, pat_urls, site=self.site)
            infered_dict = {url: list(set(infered_dict_all[url] + infered_dict_common[url])) for url in infered_dict_all}
            # self.tracer.debug(f'infered_dict: {infered_dict}')
            
            pat_infer_urls = {iu[0]: iu for iu in infer_urls[pat]} # {url: (url, (meta))}
            fp_urls = set([p[2] for p in self.pattern_dict[pat]])
            for infer_url, cand in infered_dict.items():
                # // logger.info(f'Infer url: {infer_url} {cand}')
                reorg_url, trace = self.inferer.if_reorg(infer_url, cand, fp_urls=fp_urls)
                if reorg_url is not None:
                    if not self.fp_check(infer_url, reorg_url):
                        self.tracer.info(f'Found by infer: {infer_url} --> {reorg_url}')
                        by_dict = {'method': 'infer'}
                        by_dict.update(trace)
                        # Infer
                        self.db.reorg.update_one({'url': infer_url}, {'$set': {
                            self.classname: {
                                'reorg_url': reorg_url, 
                                'by': by_dict
                            }
                        }})
                        suc = (pat_infer_urls[infer_url][0], pat_infer_urls[infer_url][1], reorg_url)
                        self._add_url_to_patterns(*unpack_ex(suc))
                        success.append(suc)
                    else: # False positive
                        try: self.db.na_urls.update_one({'_id': infer_url}, {'$set': {
                                'false_positive_infer': True,
                                'hostname': self.site
                            }}, upsert=True)
                        except: pass
                self.db.checked.update_one({'_id': infer_url}, {'$set': {f'{self.classname}.infer': True}})
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
        if self.similar.site is None or self.site not in self.similar.site:
            self.similar.clear_titles()
            if not self.similar._init_titles(self.site):
                self.tracer.warn(f"Similar._init_titles: Fail to get homepage of {self.site}")
                return
        urls = {}
        success = [None]
        for pat, examples in self.pattern_dict.items():
            for example in examples:
                url, _, _ = example
                urls[url] = example
        examples = list(urls.values())
        success = list(urls.values())
        while(len(success)) > 0:
            success = self.query_inferer(examples)
            for suc in success:
                self._add_url_to_patterns(*unpack_ex(suc))
            examples = success
            success = self.query_inferer(examples)
        self.tracer.flush()


    def interleave_infer(self, infer=True, required_urls=None, title=True):
        """
        infer: Infer every time when a new alias is found
        Required urls: URLs that will be run on (no checked)
        title: Whether title comparison is taken into consideration
        """
        self.tracer.critical(f'Site: {self.site}')
        if not title:
            self.similar.clear_titles()
        elif self.similar.site is None or self.site not in self.similar.site:
            self.similar.clear_titles()
            if not self.similar._init_titles(self.site):
                self.tracer.warn(f"Similar._init_titles: Fail to get homepage of {self.site}")
                return
        noreorg_urls = list(self.db.reorg.find({"hostname": self.site, self.classname: {"$exists": False}}))
        # searched_checked = self.db.checked.find({"hostname": self.site, f"{self.classname}.search": True})
        # searched_checked = set([sc['url'] for sc in searched_checked])
        
        required_urls = set(required_urls) if required_urls else set([u['url'] for u in noreorg_urls])

        urls = [u for u in noreorg_urls if u['url'] in required_urls]
        broken_urls = set([u['url'] for u in urls])
        self.tracer.info(f'Search SITE: {self.site} #URLS: {len(broken_urls)}')
        self.aliases = set()
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.tracer.info(f'URL: {i} Search {url}')
            reorg = self.db.reorg.find_one({'url': url})
            title = reorg.get('title', '')

            searched = None
            if SEARCH_CLASS in reorg:
                searched = reorg[SEARCH_CLASS]['reorg_url']
                self.tracer.info(f"HIT: {searched}")
                fp = self.fp_check(url, searched)
                if fp: # False positive test
                    searched = None

            if searched is not None:
                self.aliases.add(url)
            if searched is not None and infer:
                example = (url, (title,), searched)
                added = self._add_url_to_patterns(*unpack_ex(example))
                if added:
                    success = self.query_inferer([example])
                    while len(success) > 0:
                        added = False
                        for suc in success:
                            broken_urls.discard(unpack_ex(suc)[0])
                            self.aliases.add(unpack_ex(suc)[0])
                            a = self._add_url_to_patterns(*unpack_ex(suc))
                            added = added or a
                        if not added: 
                            break
                        examples = success
                        success = self.query_inferer(examples)
                self.tracer.flush()
            if searched is not None:
                continue
            
            self.tracer.info(f'URL: {i} Discover {url}')
            reorg = self.db.reorg.find_one({'url': url})
            title = reorg.get('title', '')
            
            discovered = None
            if DISCOVER_CLASS in reorg:
                discovered = reorg[DISCOVER_CLASS]['reorg_url']
                self.tracer.info(f"HIT: {discovered}")
                fp = self.fp_check(url, discovered)
                if fp: # False positive test
                    discovered = None
                # * Filter out non-homepage --> homepage alias (highly likely FPs)

                def is_homepage(url):
                    us = urlsplit(url)
                    return us.path in ['', '/'] and not us.query

                if not is_homepage(url) and is_homepage(discovered):
                    discovered = None
            
            if discovered is not None:
                self.aliases.add(url)
            if discovered is not None and infer:
                example = (url, (title,), discovered)
                added = self._add_url_to_patterns(*unpack_ex(example))
                if added:
                    success = self.query_inferer([example])
                    while len(success) > 0:
                        added = False
                        for suc in success:
                            broken_urls.discard(unpack_ex(suc)[0])
                            self.aliases.add(unpack_ex(suc)[0])
                            a = self._add_url_to_patterns(*unpack_ex(suc))
                            added = added or a
                        if not added: 
                            break
                        examples = success
                        success = self.query_inferer(examples)
                self.tracer.flush()
            