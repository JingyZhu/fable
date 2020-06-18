from xmlrpc.client import ServerProxy
import pandas as pd
import pickle
from urllib.parse import urlsplit
from . import tools
from collections import defaultdict
import string
import time

import sys
sys.path.append('../')
import config
from utils import search, crawl, text_utils, url_utils

import logging
logger = logging.getLogger('logger')

class Inferer:
    def __init__(self, proxies={}, memo=None, similar=None):
        self.PS = crawl.ProxySelector(proxies)
        self.proxy = ServerProxy(config.RPC_ADDRESS, allow_none=True)
        self.memo = memo if memo is not None else tools.Memoizer()
        self.similar = similar if similar is not None else tools.Similar()

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
            li = string.digits + string.ascii_letters + ' _-'
            rs = ''
            for ch in s:
                if ch in li: rs += ch
                else: rs += ' '
            return rs
        sheets = []
        sheet1_csv = defaultdict(list)
        sheet2_csv = defaultdict(list)
        sheet3_csv = defaultdict(list)
        for ex_input, reorg_url in examples:
            url, meta = ex_input
            us = urlsplit(url)
            path_list = list(filter(lambda x: x != '', us.path.split('/')))
            url_inputs = [us.netloc.split(':')[0]] + path_list
            sheet2_csv['Site'].append(us.netloc.split(':')[0])
            if us.query: url_inputs.append(us.query)
            for i, url_piece in enumerate(url_inputs):
                sheet1_csv[f'URL{i}'].append(url_piece)
                sheet3_csv[f'URL{i}'].append(url_piece)
            if isinstance(meta, tuple):
                for i, meta_piece in enumerate(meta):
                    sheet1_csv[f'Meta{i}'].append(normal(meta_piece))
                    sheet2_csv[f'Meta{i}'].append(normal(meta_piece))
            elif isinstance(meta, str):
                sheet1_csv[f'Meta0'].append(normal(meta))
                sheet2_csv[f'Meta0'].append(normal(meta))
            sheet1_csv['Output'].append(reorg_url)
            sheet2_csv['Output'].append(reorg_url)
            sheet3_csv['Output'].append(reorg_url)
        urls_idx = {}
        for i, (url, meta) in enumerate(urls):
            us = urlsplit(url)
            urls_idx[url] = i + len(examples)
            # sheet1_csv['URL'].append(url)
            path_list = list(filter(lambda x: x != '', us.path.split('/')))
            url_inputs = [us.netloc.split(':')[0]] + path_list
            sheet2_csv['Site'].append(us.netloc.split(':')[0])
            if us.query: url_inputs.append(us.query)
            for i, url_piece in enumerate(url_inputs):
                sheet1_csv[f'URL{i}'].append(url_piece)
                sheet3_csv[f'URL{i}'].append(url_piece)
            if isinstance(meta, tuple):
                for i, meta_piece in enumerate(meta):
                    sheet1_csv[f'Meta{i}'].append(normal(meta_piece))
                    sheet2_csv[f'Meta{i}'].append(normal(meta_piece))
            elif isinstance(meta, str):
                sheet1_csv[f'Meta0'].append(normal(meta))
                sheet2_csv[f'Meta0'].append(normal(meta))
            sheet1_csv['Output'].append('')
            sheet2_csv['Output'].append('')
            sheet3_csv['Output'].append('')
        sheets = [sheet1_csv, sheet2_csv, sheet3_csv]
        sheets = [pickle.dumps({
            'sheet_name': f'sheet{i+1}',
            'csv': sheet
        }) for i, sheet in enumerate(sheets)]
        count = 0
        while count < 3:
            try:
                outputs = self.proxy.handle(sheets, site)
                break
            except Exception as e:
                logger.error(f'infer: exception on RPC {str(e)} {pickle.loads(sheets[0])}')
                count += 1
                time.sleep(2)
                continue
        outputs = [pickle.loads(o.data) for o in outputs]
        outputs = [pd.DataFrame(o['csv']) for o in outputs]
        poss_infer = defaultdict(set)
        for output in outputs:
            for url, _ in urls:
                idx = urls_idx[url]
                reorg_url = output.iloc[idx]['Output']
                if not isinstance(reorg_url, str): continue
                poss_infer[url].add(reorg_url)
        return {k: list(v) for k, v in poss_infer.items()}
    
    def if_reorg(self, url, reorg_urls):
        """
        reorg_urls: all urls infered by inferer
        """
        reorg_content = {}
        reorg_title = {}
        for reorg_url in reorg_urls:
            reorg_html = self.memo.crawl(reorg_url)
            if reorg_html is None:
                continue
            reorg_content[reorg_url] = self.memo.extract_content(reorg_html)
            reorg_title[reorg_url] = self.memo.extract_title(reorg_html)
        if len(reorg_content) + len(reorg_title) == 0:
            return None, "reorg pages not exists"
        wayback_url = self.memo.wayback_index(url)
        html = self.memo.crawl(wayback_url)
        if html is None: return None, "url fail to load on wayback"
        content = self.memo.extract_content(html)
        title = self.memo.extract_title(html)
        similars = self.similar.similar(url, title, content, reorg_title, reorg_content)
        if len(similars) > 0:
            return similars[0]
        else:
            return None, "no similar pages"
