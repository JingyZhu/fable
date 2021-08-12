from xmlrpc.client import ServerProxy
import pandas as pd
import numpy as np
import pickle
from urllib.parse import urlsplit, parse_qsl, parse_qs
from collections import defaultdict
import string
import time
import socket
import os
import regex

from . import config, tools, tracer
from .utils import crawl, sic_transit, url_utils

import logging
logging.setLoggerClass(tracer.tracer)
tracer = logging.getLogger('logger')
logging.setLoggerClass(logging.Logger)

ISNUM = lambda x: type(x).__module__ == np.__name__ or isinstance(x, int)
VERTICAL_BAR_SET = '\u007C\u00A6\u2016\uFF5C\u2225\u01C0\u01C1\u2223\u2502\u0964\u0965'


def normal_hostname(hostname):
    hostname = hostname.split(':')[0]
    hostname = hostname.split('.')
    if hostname[0] == 'www': hostname = hostname[1:]
    return '.'.join(hostname)


class Inferer:
    def __init__(self, proxies={}, memo=None, similar=None):
        self.PS = crawl.ProxySelector(proxies)
        self.proxy = ServerProxy(config.RPC_ADDRESS, allow_none=True)
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()
        self.not_workings = set() # Seen broken inferred URLs
        self.site = None
        self.seen_reorg_pairs = None
        self.pattern_dict = None

    def init_site(self, site):
        if self.site:
            self.clear_site()
        self.site = site
        self.seen_reorg_pairs = set()
        self.pattern_dict = defaultdict(list)

    def clear_site(self):
        self.site = None
        self.seen_reorg_pairs = None
        self.pattern_dict = None

    def _add_url_to_patterns(self, url, meta, reorg):
        """
        Only applies to same domain currently
        meta: [title]
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
        if meta[0] == 'N/A':
            meta[0] = ''
        for pat in patterns:
            self.pattern_dict[pat].append((url, meta, reorg))
        return True

    def _most_common_output(self, examples):
        """
        Given a list of examples, return ones with highest # common pattern

        Return: List of examples in highest common pattern
        """
        output_patterns = defaultdict(list)
        for ex in examples:
            reorg_url = ex[2]
            reorg_pats = url_utils.gen_path_pattern(reorg_url)
            for reorg_pat in reorg_pats:
                output_patterns[reorg_pat].append(ex)
        output_patterns = sorted(output_patterns.items(), key=lambda x:len(x[1]), reverse=True)
        output_pattern, output_ex = output_patterns[0]
        self.tracer.debug(f"_most_common_output: {output_pattern} {len(output_ex)} {len(examples)}")
        return output_ex

    def infer(self, examples, urls, site='NA'):
        """
        Infer reorg urls of urls by learning the transformation rule in urls
        examples: list of ((urls, (other metadata)), reorg_url)
        urls: list of (urls, other metadata)
        Two metadata should be in the same format

        Returns: {url: [possible reorg_url]}
        # TODO: Create more sheets with similar/same #words
        """ 
        def normal(s):
            tokens = regex.split(f'_| [{VERTICAL_BAR_SET}] |[{VERTICAL_BAR_SET}]| \p{{Pd}} ', s)
            if len(tokens) > 1:
                s = tokens[0]
            li = string.digits + string.ascii_letters + ' _-'
            rs = ''
            for ch in s:
                if ch in li: rs += ch
                elif ch == "'": continue
                else: rs += ' '
            return rs
        
        def insert_url(sheet, row, url):
            """Insert the original (broken) URL part into the sheet"""
            us = urlsplit(url)
            path_list = list(filter(lambda x: x != '', us.path.split('/')))
            url_inputs = [normal_hostname(us.netloc)] + path_list
            for j, url_piece in enumerate(url_inputs):
                sheet.loc[row, f'URL{j}'] = url_piece
                qs = url_utils.my_parse_qs(us.query)
            for key, value in qs.items():
                if key == 'NoKey':
                    sheet.loc[row, f'Query_{key}'] = value[0]
                else:
                    sheet.loc[i, f'Query_{key}'] = f'{key}={value[0]}'
            return sheet

        def insert_metadata(sheet, row, meta, expand=True):
            """Expand: Whether to expand the metadata into different form"""
            for j, meta_piece in enumerate(meta):
                if expand:
                    sheet.loc[row, f'Meta{j}'] = normal(meta_piece)
                    sheet.loc[row, f'Meta{j+0.5}'] = normal(meta_piece.lower())
                else:
                    sheet.loc[row, f'Meta{j}'] = meta_piece
            return sheet
        
        def insert_reorg(sheet, row, reorg):
            """Insert alias part into the sheet"""
            us_reorg = urlsplit(reorg)
            path_reorg_list = list(filter(lambda x: x != '', us_reorg.path.split('/')))
            url_reorg_inputs = [f"https://{normal_hostname(us_reorg.netloc)}"] + path_reorg_list
            for j, reorg_url_piece in enumerate(url_reorg_inputs):
                sheet.loc[row, f'Output_{j}'] = reorg_url_piece
                qs_reorg = url_utils.my_parse_qs(us_reorg.query)
            for key, value in qs_reorg.items():
                if key == 'NoKey':
                    sheet.loc[i, f'Output_Q_{key}'] = value[0]
                else:
                    sheet.loc[i, f'Output_Q_{key}'] = f'{key}={value[0]}'
            return sheet
                
        sheet1 = pd.DataFrame() # Both url and meta
        sheet2 = pd.DataFrame() # Only meta
        sheet3 = pd.DataFrame() # Only URL
        # * Input examples
        for i, (url, meta, reorg_url) in enumerate(examples):
            # * Input URL part
            sheet1 = insert_url(sheet1, i, url)
            sheet3 = insert_url(sheet3, i, url)
            # * Input Metadata part
            sheet1 = insert_metadata(sheet1, i, meta, expand=True)
            sheet2 = insert_metadata(sheet2, i, meta, expand=True)
            # * Input Reorg Part
            sheet1 = insert_reorg(sheet1, i, reorg_url)
            sheet2 = insert_reorg(sheet2, i, reorg_url)
            sheet3 = insert_reorg(sheet3, i, reorg_url)

        url_idx = {}
        # * Input the to infer examples
        for i, (url, meta) in enumerate(urls):
            counter = i+len(examples)
            url_idx[url] = counter
            # * Input URL part
            sheet1 = insert_url(sheet1, counter, url)
            sheet3 = insert_url(sheet3, counter, url)
            # * Input Metadata part
            sheet1 = insert_metadata(sheet1, counter, meta, expand=True)
            sheet2 = insert_metadata(sheet2, counter, meta, expand=True)
        
        # * RPC formatted dataframe to FlashFill
        sheets = [sheet1, sheet2, sheet3]
        sheets = [pickle.dumps({
            'sheet_name': f'sheet{i+1}',
            'csv': sheet
        }) for i, sheet in enumerate(sheets)]
        count = 0
        while count < 3:
            try:
                # socket.setdefaulttimeout(20)
                outputs = self.proxy.handle(sheets, site + str(time.time()))
                # socket.setdefaulttimeout(None)
                break
            except Exception as e:
                tracer.error(f'infer: exception on RPC {str(e)}')
                count += 1
                time.sleep(2)
                continue
        if count == 3:
            return {}
        outputs = [pickle.loads(o.data) for o in outputs]
        poss_infer = defaultdict(set) # * Any results inferred from 3 sheets
        seen_reorg = set()
        for output in outputs:
            for url, meta in urls:
                idx = url_idx[url]
                reorg_url_lists = output.filter(regex='^Output_\d', axis=1).iloc[idx]
                reorg_query_lists = output.filter(regex='^Output_Q', axis=1).iloc[idx]
                num_url_outputs = len(reorg_url_lists)
                scheme_netloc = reorg_url_lists['Output_0']
                reorg_paths = []
                for j in range(1, num_url_outputs):
                    reorg_part = reorg_url_lists[f'Output_{j}']
                    # TODO: How to deal with nan requires more thoughts
                    if reorg_part != reorg_part: # * Check for NaN value (trick)
                        continue
                    if ISNUM(reorg_part): reorg_part = str(int(reorg_part))
                    reorg_paths.append(reorg_part)
                reorg_paths = '/'.join(reorg_paths)
                reorg_url = f'{scheme_netloc}/{reorg_paths}'
                reorg_queries = []
                for key in reorg_query_lists:
                    reorg_kv = reorg_query_lists[f'Output_Q_{key}']
                    if reorg_kv != reorg_kv or (key != "NoKey" and not reorg_kv.split('=')[1]):
                        continue
                    if ISNUM(reorg_kv): reorg_kv = str(int(reorg_kv))
                    reorg_queries.append(reorg_kv)
                if len(reorg_queries) > 0:
                    reorg_url += f"?{'&'.join(reorg_queries)}"
                
                if reorg_url not in seen_reorg:
                    tracer.inference(url, meta, examples, reorg_url)
                    seen_reorg.add(reorg_url)
                poss_infer[url].add(reorg_url)
        return {k: list(v) for k, v in poss_infer.items()}
    
    def infer_all(self, examples):
        """
        examples: Lists of (url, title), reorg_url. 
                Should already be inserted into self.pattern_dict
        
        returns: Returned successed (url, (meta)), reorg
        """
        if len(examples) <= 0:
            return []
        patterns = set() # All patterns in example
        for url, (title), reorg_url in examples:
            pats = url_utils.gen_path_pattern(url)
            patterns.update(pats)
        patterns = list(patterns)
        broken_urls = self.db.reorg.find({'hostname': self.site})
        # infer
        broken_urls = [reorg for reorg in broken_urls if len(set(reorg.keys()).intersection(self.inference_classes)) == 0]
        # self.db.reorg.update_many({'hostname': self.site, "title": ""}, {"$unset": {"title": ""}})
        infer_urls = defaultdict(list) # * {Pattern: [(urls, (meta,))]}
        for toinfer_url in list(broken_urls):
            for pat in patterns:
                if not url_utils.pattern_match(pat, toinfer_url['url']):
                    continue
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
                    self.inferer._add_url_to_patterns(*unpack_ex(suc))
                    success.append(suc)
        return success
    
    def if_reorg(self, url, reorg_urls, compare=True, fp_urls={}):
        """
        reorg_urls: all urls infered by inferer
        compare: whether to actually compare the content/title
        fp_urls: Used when compare=False. List of already known urls from output to avoid inference from infering on the same url
        return: Matched URLS, trace(dict)
        """
        reorg_content = {}
        reorg_title = {}
        if not compare:
            new_reorg = False
            for reorg_url in reorg_urls:
                # Try:
                if urlsplit(url).path not in ['', '/'] and urlsplit(reorg_url).path in ['', '/']:
                    continue
                # End of Try
                # match = [url_utils.url_match(reorg_url, fp_url) for fp_url in fp_urls]
                # if True in match:
                #     continue
                new_reorg = True
                if reorg_url in self.not_workings:
                    tracer.debug('Inferred URL already checked broken')
                rval, _ = sic_transit.broken(reorg_url)
                if rval == False:
                    return reorg_url, {'type': "nocomp_check", "value": 'N/A'}
                else:
                    self.not_workings.add(reorg_url)
            if not new_reorg:
                return None, {'reason': 'No new reorg actually inferred'}
            else:
                return None, {'reason': 'Inferred urls broken'}
        else:
            for reorg_url in reorg_urls:
                if reorg_url in self.not_workings:
                    tracer.debug('Inferred URL already checked broken')
                    continue
                reorg_html = self.memo.crawl(reorg_url)
                if reorg_html is None:
                    self.not_workings.add(reorg_url)
                    continue
                reorg_content[reorg_url] = self.memo.extract_content(reorg_html)
                reorg_title[reorg_url] = self.memo.extract_title(reorg_html)
            if len(reorg_content) + len(reorg_title) == 0:
                value = self.if_reorg(url, reorg_urls, compare=False, fp_urls=fp_urls)
                return value
            try:
                wayback_url = self.memo.wayback_index(url)
                html = self.memo.crawl(wayback_url)
                if html is None: return None, {"reason": "url fail to load on wayback"}
                content = self.memo.extract_content(html)
                title = self.memo.extract_title(html)
            except:
                # * No wayback archive of broken page, do no compare check
                value = self.if_reorg(url, reorg_urls, compare=False, fp_urls=fp_urls)
                return value
                # return None, {"reason": "Fail to get wayback url, html or content/title"}
            similars, fromm = self.similar.similar(wayback_url, title, content, reorg_title, reorg_content)
            if len(similars) > 0:
                top_similar = similars[0]
                return top_similar[0], {'type': fromm, 'value': top_similar[1]}
            else:
                return None, {'reason': "no similar pages"}
