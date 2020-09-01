"""
Get confidence of a url with alias
"""
from ReorgPageFinder import discoverer, searcher, inferer, tools
import os
from urllib.parse import urlsplit, urlparse, urlunsplit
from . import tools
from collections import defaultdict
import re, json
import random
from dateutil import parser as dparser
import datetime
import pymongo
from pymongo import MongoClient
import pandas as pd
import math

import sys
sys.path.append('../')
import config
from utils import search, crawl, text_utils, url_utils, sic_transit

import logging
logger = logging.getLogger('logger')

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder
he = url_utils.HostExtractor()

memo = tools.Memoizer()

def get_features(url, reorg, broken_reason=None):
    """
    Get a set of features:
    Broken reason, Wayback last good archive, #snapshots, Found method, Matched method

    return: {'search': feature, 'discover': feature}
    """
    # param_dict = {
    #     "filter": ['statuscode:[23][0-9]*', 'mimetype:text/html'],
    # }
    def cate_year(year):
        if year >= 2014:
            return "2014~"
        elif year >= 2009:
            return "2009-2014"
        elif year >= 2004:
            return "2004-2009"
        else:
            return "1999-2004"
    def cate_sps(num):
        if num == 0: return 0
        breakdown = [5, 10, 10, 50, 100, 500, 1000]
        for bd in breakdown:
            if num < bd: return bd
        return 5000

    if broken_reason is None:
        broken_reason = sic_transit.broken(url)[1]
        if isinstance(broken_reason, list): broken_reason = json.dumps(broken_reason)
        broken_reason = url_utils.status_categories(broken_reason)
    last_ts = memo.wayback_index(url, policy='latest')
    if last_ts:
        last_ts = url_utils.get_ts(last_ts)
    sps = memo.wayback_index(url, policy='all')
    sps = len(sps) if sps else 0
    feature = {
        'broken_reason': broken_reason,
        'last_ts': cate_year(dparser.parse(last_ts).year) if last_ts else 'N/A',
        'num_sps': cate_sps(sps)
    }
    features = {}
    if 'reorg_url_search' in reorg:
        feature_s = feature.copy()
        feature_s['reorg_url'] = reorg['reorg_url_search']
        feature_s['found'] = 'search'
        feature_s['matched'] = reorg['by_search']['type']
        features['search'] = feature_s
    if 'reorg_url_discover_test' in reorg:
        feature_d = feature.copy()
        feature_d['reorg_url'] = reorg['reorg_url_discover_test']
        feature_d['found'] = 'discover'
        feature_d['matched'] = reorg['by_discover_test']['type']
        features['discover'] = feature_d
    elif 'reorg_url_discover' in reorg:
        feature_d = feature.copy()
        feature_d['reorg_url'] = reorg['reorg_url_discover']
        feature_d['found'] = 'discover'
        feature_d['matched'] = reorg['by_discover']['type']
        features['discover'] = feature_d
    return features


def features_2_table(objs):
    """
    objs: {'url': , 'reason': , 'search': , 'discover', 'label': ''}
    """
    tp_map = {True: 'tp', False: 'fp'}
    constr = lambda: {'tp': 0, 'fp': 0}
    tables = {
        'broken_reason': defaultdict(constr),
        'last_ts': defaultdict(constr),
        'num_sps': defaultdict(constr),
        'found': defaultdict(constr),
        'matched': defaultdict(constr)
    }
    for i, obj in enumerate(objs):
        print(i, obj['url'])
        reorg = db.reorg.find_one({'url': obj['url']})
        features = get_features(obj['url'], reorg, obj['reason'])
        assert(not (obj['label'] == 'TRUE' and ('by_search' in reorg and 'by_discover_test' in reorg)))
        if obj['search']:
            label_s = obj['label'] in ['TRUE', 'TRUE_S', 'TRUE_B']
            label_s = tp_map[label_s]
            for k, v in features['search'].items():
                if k == 'reorg_url': continue
                tables[k][v][label_s] += 1
        if obj['discover']:
            label_s = obj['label'] in ['TRUE', 'TRUE_D', 'TRUE_B']
            label_s = tp_map[label_s]
            for k, v in features['discover'].items():
                if k == 'reorg_url': continue
                tables[k][v][label_s] += 1
    for k, v in tables.items():
        print(k)
        df = pd.DataFrame(v)
        print(df)
    json.dump(tables, open('tables.json', 'w+'), indent=2)

