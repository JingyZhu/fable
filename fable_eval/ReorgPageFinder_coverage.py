import searcher_coverage, discoverer_efficiency, inferer_efficiency, tools, confidence
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit, parse_qsl
import os
from collections import defaultdict
import time
import json
import logging
import sys

from fable import config
from fable.utils import text_utils, url_utils, crawl, sic_transit

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder
he = url_utils.HostExtractor()

KEYNAME = 'infer_basic'
CHECK_NAME = 'infer_efficiency_basic'

def get_reorg(reorg):
    found_keys = ['reorg_url_search', 'reorg_url_discover', 'reorg_url_infer', 'reorg_url_discover_test']
    reorg_urls = []
    for fk in found_keys:
        if fk in reorg: reorg_urls.append(reorg[fk])
    return reorg_urls


def unpack_ex(ex):
    (url, title), reorg = ex
    return url, title, reorg


def gen_path_pattern(url, dis=1):
    """
    Generate path patterns where all paths with same edit distance should follow
    # TODO: Currently only support edit distance of 1,  Could have larger dis
    """
    us = urlsplit(url)
    us = us._replace(netloc=us.netloc.split(':')[0])
    if us.path == '':
        us = us._replace(path='/')
    if us.path[-1] == '/' and us.path != '/':
        us = us._replace(path=us.path[:-1])
    path_lists = list(filter(lambda x: x!= '', us.path.split('/')))
    if us.query: 
        path_lists.append(us.query)
    patterns = []
    patterns.append(tuple(['*'] + path_lists))
    for i in range(len(path_lists)):
        path_copy = path_lists.copy()
        path_copy[i] = '*'
        patterns.append(tuple([us.netloc] + path_copy))
    return patterns


def pattern_match(pattern, url, dis=1):
    us = urlsplit(url)
    us = us._replace(netloc=us.netloc.split(':')[0])
    if us.path == '':
        us = us._replace(path='/')
    if us.path[-1] == '/' and us.path != '/':
        us = us._replace(path=us.path[:-1])
    path_lists = list(filter(lambda x: x!= '', us.path.split('/')))
    path_lists = [us.netloc] + path_lists
    if us.query: 
        path_lists.append(us.query)
    if len(pattern) != len(path_lists):
        return False
    for pat, path in zip(pattern, path_lists):
        if pat == '*': continue
        elif pat != path: return False
    return True


class ReorgPageFinder:
    def __init__(self, use_db=True, db=db, memo=None, similar=None, proxies={}, logger=None, logname=None):
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.PS = crawl.ProxySelector(proxies)
        self.searcher = searcher_coverage.Searcher(memo=self.memo, similar=self.similar, proxies=proxies)
        self.discoverer = discoverer_efficiency.Discoverer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.inferer = inferer_efficiency.Inferer(memo=self.memo, similar=self.similar, proxies=proxies)
        self.db = db
        self.site = None
        self.pattern_dict = None
        self.seen_reorg_pairs = None
        self.logname = './ReorgPageFinder.log' if logname is None else logname
        self.logger = logger if logger is not None else self._init_logger()

    def _init_logger(self):
        logger = logging.getLogger('logger')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s %(asctime)s [%(filename)s %(funcName)s:%(lineno)s]: \n %(message)s')
        file_handler = logging.FileHandler(self.logname)
        file_handler.setFormatter(formatter)
        std_handler = logging.StreamHandler(sys.stdout)
        std_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(std_handler)
        return logger
    
    def init_site(self, site):
        self.site = site
        self.pattern_dict = defaultdict(list)
        self.seen_reorg_pairs = set()
    
    def init_site_infer(self, site):
        self.site = site
        self.pattern_dict = defaultdict(list)
        self.seen_reorg_pairs = set()

        reorg_keys = {'reorg_url', 'reorg_url_search', 'reorg_url_discover_test', 'reorg_url_discover', 'reorg_url_infer'}
        reorg_urls = self.db.reorg_infer.find({'hostname': site})
        reorg_urls = [reorg for reorg in reorg_urls if len(set(reorg.keys()).intersection(reorg_keys)) > 0]
        for reorg_url in list(reorg_urls):
            if 'title' not in reorg_url:
                try:
                    wayback_reorg_url = self.memo.wayback_index(reorg_url['url'])
                    reorg_html, wayback_reorg_url = self.memo.crawl(wayback_reorg_url, final_url=True)
                    reorg_title = self.memo.extract_title(reorg_html, version='domdistiller')
                except:
                    reorg_title = 'N/A'
                reorg_url['title'] = reorg_title
                self.db.reorg_infer.update_one({'url': reorg_url['url']}, {'$set': {'title': reorg_title}})
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)

    def _add_url_to_patterns(self, url, title, reorg):
        """
        Only applies to same domain currently
        Return bool on whether success
        """
        # if he.extract(reorg) != he.extract(url):
        #     return False
        patterns = gen_path_pattern(url)
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
            reorg_pats = gen_path_pattern(reorg_url)
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
        patterns = set()
        for (url, title), reorg_url in examples:
            pats = gen_path_pattern(url)
            patterns.update(pats)
        patterns = list(patterns)
        broken_urls = self.db.reorg_infer.find({'hostname': self.site})
        # infer
        infer_checked = self.db.checked.find({"hostname": self.site, CHECK_NAME: True})
        infer_checked = set([ic['url'] for ic in infer_checked])
        broken_urls = [reorg for reorg in broken_urls if reorg['url'] not in infer_checked]
        # self.db.reorg_infer.update_many({'hostname': self.site, "title": ""}, {"$unset": {"title": ""}})
        infer_urls = defaultdict(list) # Pattern: [(urls, (meta))]
        for infer_url in list(broken_urls):
            for pat in patterns:
                if not pattern_match(pat, infer_url['url']):
                    continue
                if 'title' not in infer_url:
                    try:
                        wayback_infer_url = self.memo.wayback_index(infer_url['url'])
                        wayback_infer_html = self.memo.crawl(wayback_infer_url)
                        title = self.memo.extract_title(wayback_infer_html)
                        self.db.reorg_infer.update_one({'_id': infer_url['_id']}, {'$set': {'title': title}})
                    except Exception as e:
                        self.logger.error(f'Exceptions happen when loading wayback verison of url: {str(e)}') 
                        title = ""
                else: title = infer_url['title'] 
                infer_urls[pat].append((infer_url['url'], (title)))
        if len(infer_urls) <=0:
            return []
        success = []
        for pat, pat_urls in infer_urls.items():
            self.logger.info(f'Pattern: {pat}')
            infered_dict_all = self.inferer.infer(self.pattern_dict[pat], pat_urls, site=self.site)
            common_output = self._most_common_output(self.pattern_dict[pat])
            # print(common_output)
            infered_dict_common = self.inferer.infer(common_output, pat_urls, site=self.site)
            infered_dict = {url: list(set(infered_dict_all[url] + infered_dict_common[url])) for url in infered_dict_all}
            self.logger.info(f'infered_dict: {json.dumps(infered_dict, indent=2)}')
            
            pat_infer_urls = {iu[0]: iu for iu in infer_urls[pat]} # url: pattern
            fp_urls = set([p[1] for p in self.pattern_dict[pat]])

            for infer_url, cand in infered_dict.items():
                ground_truth_urls = get_reorg(self.db.reorg_infer.find_one({'url': infer_url}))
                for ground_truth_url in ground_truth_urls:
                    reorg_url, trace = self.inferer.if_reorg_check(infer_url, cand, ground_truth_url)
                    if reorg_url == 'N/A':
                        break
                    elif reorg_url is not None:
                        self.logger.info(f'Found by infer: {infer_url} --> {reorg_url}')
    
                        by_dict = {'method': 'infer'}
                        by_dict.update(trace)
                        # Infer
                        self.db.reorg_infer.update_one({'url': infer_url}, {'$set': {
                            f'reorg_url_{KEYNAME}': reorg_url, 
                            f'by_{KEYNAME}': by_dict
                        }})
                        suc = ((pat_infer_urls[infer_url]), reorg_url)
                        self._add_url_to_patterns(*unpack_ex(suc))
                        success.append(suc)

                        self.db.checked.update_one({'url': infer_url}, {'$set': {CHECK_NAME: True}})
                        break

                # if reorg_url != 'N/A':
                #     self.db.checked.update_one({'_id': infer_url}, {'$set': {CHECK_NAME: True}})
        return success


    def search_by_queries(self, site, required_urls):
        required_urls = set(required_urls)
        site_urls = db.reorg.find({"hostname": site})
        searched_checked = db.checked.find({"hostname": self.site, "search_coverage": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        urls = [u for u in site_urls if u['url'] not in searched_checked and u['url'] in required_urls]
        broken_urls = set([(u['url'], u.get('reorg_url_search')) for u in urls])
        self.logger.info(f'Search coverage SITE: {site} #URLS: {len(broken_urls)}')
        i = 0
        self.similar.clear_titles()
        while len(broken_urls) > 0:
            url,reorg_url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            # TODO Change with requirements
            search_trace = self.searcher.search_title_exact_site(url)
            if search_trace is None:
                continue

            search_trace.update({
                'url': url,
                'hostname': he.extract(url)
            })
            if reorg_url:
                search_trace.update({'reorg_url': reorg_url})
            try:
                self.db.search_trace.update_one({'_id': url}, {'$set': search_trace}, upsert=True)
            except Exception as e:
                self.logger.warn(f'Search_cover update search_trace: {str(e)}')
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    "search_coverage": True
                }}, upsert=True)
            except Exception as e:
                self.logger.warn(f'Search_cover update checked: {str(e)}')
    
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

    def search_gt_10(self, site, required_urls):
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        required_urls = set(required_urls)
        site_urls = list(db.reorg.find({"hostname": site}))
        searched_checked = db.checked.find({"hostname": self.site, "search_gt_10": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        urls = [u for u in site_urls if u['url'] not in searched_checked and u['url'] in required_urls]
        broken_urls = set([u['url'] for u in urls])
        self.logger.info(f'Search gt 10 SITE: {site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            searched = self.searcher.search_gt_10(url, search_engine='bing')
            if searched is None:
                searched = self.searcher.search_gt_10(url, search_engine='google')
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
                    self.logger.error(f'WB_Error {url}: Fail to get data from wayback')
                    continue
                update_dict = {'title': title}
            else:
                title = has_title['title']

            if searched is not None:
                searched, trace = searched
                self.logger.info(f"HIT_gt_10: {searched}")
                fp = self.fp_check(url, searched)
                if not fp: # False positive test
                    # _search
                    update_dict.update({'reorg_url_search_gt_10': searched, 'by_search_gt_10':{
                        "method": "search"
                    }})
                    update_dict['by_search_gt_10'].update(trace)


            if len(update_dict) > 0:
                try:
                    self.db.reorg.update_one({'url': url}, {"$set": update_dict}) 
                except Exception as e:
                    self.logger.warn(f'Second search update DB: {str(e)}')
            searched_checked.add(url)
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    "search_gt_10": True
                }}, upsert=True)
            except: pass

    def discover(self, site, required_urls, search_type):
        if self.similar.site is None or self.similar.site != self.site:
            self.similar.clear_titles()
            self.similar._init_titles(self.site)
        # _discover
        assert(search_type in {'BFS', 'DFS'})
        required_urls = set(required_urls)
        site_urls = self.db.reorg.find({"hostname": self.site})
        discovered_checked = self.db.checked.find({"hostname": self.site, f"discover_{search_type}": True})
        discovered_checked = set([sc['url'] for sc in discovered_checked])
        urls = [u for u in site_urls if u['url'] not in discovered_checked and u['url'] in required_urls]
        broken_urls = set([(u['url'], u.get('by_discover_test', u.get('by_discover', {'type': 'title'}))['type']) for u in urls])
        self.logger.info(f'Discover SITE: {self.site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url, reorg_type = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            method, suffice = 'discover', False
            while True: # Dummy while lloop served as goto

                # discovered, trace = self.discoverer.bf_find(url, policy='latest')
                # if trace.get('backpath'):
                #     try:
                #         self.db.trace.update_one({'_id': url}, {"$set": {
                #             "url": url,
                #             "hostname": self.site,
                #             "backpath_latest": trace['backpath']
                #         }}, upsert=True)
                #     except Exception as e:
                #         self.logger.warn(f'Discover update trace backpath: {str(e)}')
                # if discovered:
                #     method = 'backpath_latest'
                #     break

                discovered, trace = self.discoverer.discover(url, search_type=search_type, reorg_type=reorg_type)
                try:
                    self.db.trace.update_one({'_id': url}, {"$set": {
                        "url": url,
                        "hostname": self.site,
                        f"discover_{search_type}": trace['trace']
                    }}, upsert=True)
                except Exception as e:
                    self.logger.warn(f'Discover update trace discover: {str(e)}')
                if discovered:
                    break
                suffice = trace['suffice']

                # discovered, trace = self.discoverer.bf_find(url, policy='earliest')
                # if trace.get('backpath'):
                #     try:
                #         self.db.trace.update_one({'_id': url}, {"$set": {
                #             "url": url,
                #             "hostname": self.site,
                #             "backpath_earliest": trace['backpath']
                #         }}, upsert=True)
                #     except Exception as e:
                #         self.logger.warn(f'Discover update trace backpath: {str(e)}')
                # if discovered:
                #     method = 'backpath_earliest'
                #     break
                break

            # update_dict = {}
            # has_title = self.db.reorg.find_one({'url': url})

            # if 'title' not in has_title:
            #     try:
            #         wayback_url = self.memo.wayback_index(url)
            #         html = self.memo.crawl(wayback_url)
            #         title = self.memo.extract_title(html, version='domdistiller')
            #     except: # No snapthost on wayback
            #         self.logger.error(f'WB_Error {url}: Fail to get data from wayback')
            #         try: self.db.na_urls.update_one({'_id': url}, {"$set": {
            #             'url': url,
            #             'hostname': self.site,
            #             'no_snapshot': True
            #         }}, upsert=True)
            #         except: pass
            #         title = 'N/A'
            #     update_dict = {'title': title}
            # else:
            #     title = has_title['title']


            discovered_checked.add(url)
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    f"discover_{search_type}": True
                }}, upsert=True)
            except Exception as e:
                self.logger.warn(f'Discover update checked: {str(e)}')
    
    def infer_urls(self, site, required_urls=None):
        site_urls = list(self.db.reorg_infer.find({"hostname": site}))
        infer_checked = self.db.checked.find({"hostname": self.site, CHECK_NAME: True})
        infer_checked = set([ic['url'] for ic in infer_checked])

        r_urls = set(required_urls) if required_urls else [u['url'] for u in site_urls]

        urls = [u for u in site_urls if u['url'] not in infer_checked and u['url'] in r_urls]
        broken_urls = set([u['url'] for u in urls])
        self.logger.info(f'Search coverage SITE: {site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            ri = self.db.reorg_infer.find_one({'url': url})
            if 'reorg_url_search' not in ri:
                continue
            
            # try:
            #     self.db.checked.update_one({'_id': url}, {"$set": {
            #         "url": url,
            #         "hostname": self.site,
            #         "infer_efficiency": True
            #     }}, upsert=True)
            # except Exception as e:
            #     self.logger.warn(f'Infer_eff update checked: {str(e)}')

            example = ((url, ri['title']), ri['reorg_url_search'])
            added = self._add_url_to_patterns(*unpack_ex(example))
            if not added: 
                continue
            self.db.checked.update_one({'url': url}, {'$set': {CHECK_NAME:True}})
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
        
        site_urls = list(self.db.reorg_infer.find({"hostname": site}))
        infer_checked = self.db.checked.find({"hostname": self.site, CHECK_NAME: True})
        infer_checked = set([ic['url'] for ic in infer_checked])

        r_urls = set(required_urls) if required_urls else [u['url'] for u in site_urls]

        urls = [u for u in site_urls if u['url'] not in infer_checked and u['url'] in r_urls]
        broken_urls = set([u['url'] for u in urls])
        self.logger.info(f'Discover coverage SITE: {site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            ri = self.db.reorg_infer.find_one({'url': url})
            if 'reorg_url_discover' not in ri and 'reorg_url_discover_test' not in ri:
                continue
            
            example = ((url, ri['title']), ri.get('reorg_url_discover_test', ri.get('reorg_url_discover')))

            added = self._add_url_to_patterns(*unpack_ex(example))
            if not added: 
                continue
            self.db.checked.update_one({'url': url}, {'$set': {CHECK_NAME: True}})
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
    
    def search_outlinks(self, site, required_urls):
        """
        Search for same content but for different contents
        """
        required_urls = set(required_urls)
        try:
            self.db.outlinks.insert_many([{
                "_id": u,
                "url": u,
                "hostname": site
            } for u in required_urls], ordered=False)
        except:
            pass
        site_urls = list(db.outlinks.find({"hostname": site}))
        searched_checked = db.checked.find({"hostname": self.site, "outlink_matter": True})
        searched_checked = set([sc['url'] for sc in searched_checked])
        urls = [u for u in site_urls if u['url'] not in searched_checked and u['url'] in required_urls]
        broken_urls = set([u['url'] for u in urls])
        self.logger.info(f'Matter outlinks: {site} #URLS: {len(broken_urls)}')
        i = 0
        while len(broken_urls) > 0:
            url = broken_urls.pop()
            i += 1
            self.logger.info(f'URL: {i} {url}')
            similarities = self.searcher.similar_outlinks(url)
            if len(similarities) == 0:
                simi = 0
                most_simi = 'N/A'
            else:
                most_simi, simi = similarities[0]
            update_dict = {}
            has_title = self.db.outlinks.find_one({'url': url})
            # if has_title is None: # No longer in reorg (already deleted)
            #     continue
            if 'title' not in has_title or has_title['title'] == 'N/A':
                try:
                    wayback_url = self.memo.wayback_index(url)
                    html = self.memo.crawl(wayback_url)
                    title = self.memo.extract_title(html, version='domdistiller')
                except: # No snapthost on wayback
                    self.logger.error(f'WB_Error {url}: Fail to get data from wayback')
                    try:
                        self.db.na_urls.update_one({'_id': url}, {'$set': {
                            'no_snapshot': True,
                            "hostname": site,
                            'url': url
                        }}, upsert=True)
                    except: pass
                    continue
                update_dict = {'title': title}
            else:
                title = has_title['title']

            update_dict.update({
                'most_similar': most_simi, 
                'similarity': simi,
                "hostname": site,
                "url": url
            })
            if len(update_dict) > 0:
                try:
                    self.db.outlinks.update_one({'_id': url}, { "$set": update_dict}, upsert=True) 
                except Exception as e:
                    self.logger.warn(f'outlinks matters update DB: {str(e)}')
            searched_checked.add(url)
            try:
                self.db.checked.update_one({'_id': url}, {"$set": {
                    "url": url,
                    "hostname": self.site,
                    "outlink_matter": True
                }}, upsert=True)
            except: pass
