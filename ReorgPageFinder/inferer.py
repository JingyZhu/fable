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
        for ex_input, reorg_url in examples:
            url, meta = ex_input
            us = urlsplit(url)
            path_list = list(filter(lambda x: x != '', us.path.split('/')))
            url_inputs = [us.netloc.split(':')[0]] + path_list
            sheet2_csv['Site'].append(us.netloc.split(':')[0])
            if us.query: url_inputs.append(us.query)
            for i, url_piece in enumerate(url_inputs):
                sheet1_csv[f'URL{i}'].append(url_piece)
            if isinstance(meta, tuple):
                for i, meta_piece in enumerate(meta):
                    sheet1_csv[f'Meta{i}'].append(normal(meta_piece))
                    sheet2_csv[f'Meta{i}'].append(normal(meta_piece))
            elif isinstance(meta, str):
                sheet1_csv[f'Meta0'].append(normal(meta))
                sheet2_csv[f'Meta0'].append(normal(meta))
            sheet1_csv['Output'].append(reorg_url)
            sheet2_csv['Output'].append(reorg_url)
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
            if isinstance(meta, tuple):
                for i, meta_piece in enumerate(meta):
                    sheet1_csv[f'Meta{i}'].append(normal(meta_piece))
                    sheet2_csv[f'Meta{i}'].append(normal(meta_piece))
            elif isinstance(meta, str):
                sheet1_csv[f'Meta0'].append(normal(meta))
                sheet2_csv[f'Meta0'].append(normal(meta))
            sheet1_csv['Output'].append('')
            sheet2_csv['Output'].append('')
        sheet1 = {
            'sheet_name': 'sheet1',
            'csv': sheet1_csv
        }
        sheet2 = {
            'sheet_name': 'sheet2',
            'csv': sheet2_csv
        }
        sheets.append(pickle.dumps(sheet1))
        sheets.append(pickle.dumps(sheet2))
        count = 0
        while count < 3:
            try:
                outputs = self.proxy.handle(sheets, site)
                break
            except Exception as e:
                print('infer: exception on RPC', str(e))
                count += 1
                time.sleep(2)
                continue
        outputs = [pickle.loads(o.data) for o in outputs]
        outputs = [pd.DataFrame(o['csv']) for o in outputs]
        poss_infer = defaultdict(list)
        for output in outputs:
            for url, _ in urls:
                idx = urls_idx[url]
                reorg_url = output.iloc[idx]['Output']
                poss_infer[url].append(reorg_url)
        return poss_infer
