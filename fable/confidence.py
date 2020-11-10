"""
Get confidence of a url with alias
"""
from . import discoverer, searcher, inferer, tools
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
import math, copy

from . import config
from .utils import search, crawl, text_utils, url_utils, sic_transit

import logging
logger = logging.getLogger('logger')

db = config.DB
he = url_utils.HostExtractor()

memo = tools.Memoizer()

def merge_tag(tag):
    merge_dict = {
        'link_sig': 'link_anchor',
        'earliest': 'link_anchor',
        'latest': 'link_anchor',
        'backpath_earliest': 'discover'
    }
    return merge_dict[tag] if tag in merge_dict else str(tag)


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
    
    def to_home(url, reorg_url):
        url_home = urlsplit(url).path in ['', '/'] and not urlsplit(url).query
        reorg_home = urlsplit(reorg_url).path in ['', '/'] and not urlsplit(reorg_url).query
        return not url_home and reorg_home

    if broken_reason is None:
        broken_reason = sic_transit.broken(url)[1]
        if isinstance(broken_reason, list): broken_reason = json.dumps(broken_reason)
        broken_reason = url_utils.status_categories(broken_reason)
    # last_ts = memo.wayback_index(url, policy='latest')
    # if last_ts:
    #     last_ts = url_utils.get_ts(last_ts)
    sps = memo.wayback_index(url, policy='all')
    sps = len(sps) if sps else 0
    feature = {
        'broken_reason': broken_reason,
        # 'last_ts': cate_year(dparser.parse(last_ts).year) if last_ts else 'N/A',
        'num_sps': cate_sps(sps),
    }
    features = {}
    if 'reorg_url_search' in reorg:
        feature_s = feature.copy()
        feature_s['reorg_url'] = reorg['reorg_url_search']
        feature_s['to_home'] = to_home(url, reorg['reorg_url_search'])
        feature_s['found'] = 'search'
        feature_s['matched'] = reorg['by_search']['type']
        feature_s = {k: merge_tag(v) for k, v in feature_s.items()}
        features['search'] = feature_s
    if 'reorg_url_discover_test' in reorg:
        feature_d = feature.copy()
        feature_d['reorg_url'] = reorg['reorg_url_discover_test']
        feature_d['to_home'] = to_home(url, reorg['reorg_url_discover_test'])
        feature_d['found'] = 'discover'
        feature_d['matched'] = reorg['by_discover_test']['type']
        feature_d = {k: merge_tag(v) for k, v in feature_d.items()}
        features['discover'] = feature_d
    elif 'reorg_url_discover' in reorg:
        feature_d = feature.copy()
        feature_d['reorg_url'] = reorg['reorg_url_discover']
        feature_d['to_home'] = to_home(url, reorg['reorg_url_discover'])
        feature_d['found'] = 'discover'
        feature_d['matched'] = reorg['by_discover']['type']
        feature_d = {k: merge_tag(v) for k, v in feature_d.items()}
        features['discover'] = feature_d
    if 'reorg_url' in reorg:
        feature_s = feature.copy()
        feature_s['reorg_url'] = reorg['reorg_url']
        feature_s['to_home'] = to_home(url, reorg['reorg_url'])
        feature_s['found'] = reorg['by']['method']
        feature_s['matched'] = reorg['by']['type']
        feature_s = {k: merge_tag(v) for k, v in feature_s.items()}
        features['original'] = feature_s
    return features


def features_2_table(objs):
    """
    objs: {'url': , 'reason': , 'search': , 'discover', 'label': ''}
    """
    tp_map = {True: 'tp', False: 'fp'}
    constr = lambda: {'tp': 0, 'fp': 0}
    tables = {
        "TP_FP": {'tp': 0, 'fp': 0},
        'broken_reason': defaultdict(constr),
        # 'last_ts': defaultdict(constr),
        'num_sps': defaultdict(constr),
        'found': defaultdict(constr),
        'matched': defaultdict(constr),
        'to_home': defaultdict(constr)
    }
    for i, obj in enumerate(objs):
        print(i, obj['url'])
        reorg = db.reorg.find_one({'url': obj['url']})
        features = get_features(obj['url'], reorg, obj['reason'])
        assert(not (obj['label'] == 'TRUE' and ('by_search' in reorg and 'by_discover_test' in reorg)))
        if obj['search']:
            label = obj['label'] in ['TRUE', 'TRUE_S', 'TRUE_B']
            label = tp_map[label]
            tables['TP_FP'][label] += 1
            for k, v in features['search'].items():
                if k == 'reorg_url': continue
                tables[k][v][label] += 1
        if obj['discover']:
            label = obj['label'] in ['TRUE', 'TRUE_D', 'TRUE_B']
            label = tp_map[label]
            tables['TP_FP'][label] += 1
            for k, v in features['discover'].items():
                if k == 'reorg_url': continue
                tables[k][v][label] += 1
    for k, v in tables.items():
        if k == 'TP_FP': continue
        print(k)
        df = pd.DataFrame(v)
        print(df)
    from os.path import abspath, dirname, join
    table_path = join(dirname(abspath(__file__)), '.tables.json')
    json.dump(tables, open(table_path, 'w+'), indent=2)


def confidence(url, reorg, broken_reason=None, r_feature=False):
    """
    Calculate the confidence given url and reorg by using Bayesian Inference
    r_feature: Whether features is also returned

    Return {type: (url, reorg_url, confidence)}
    """
    from os.path import abspath, dirname, join
    table_path = join(dirname(abspath(__file__)), '.tables.json')
    tables = json.load(open(table_path, 'r'))
    tp, fp = tables['TP_FP']['tp'], tables['TP_FP']['fp']
    tables = {k: pd.DataFrame(v) for k, v in tables.items()if k != 'TP_FP'}
    features = get_features(url, reorg, broken_reason)
    f = copy.deepcopy(features)
    cfds = {}
    for typee, feature in features.items():
        tp_rate, fp_rate = 1, 1
        reorg_url = feature['reorg_url']
        del(feature['reorg_url'])
        for k, v in feature.items():
            table = tables[k]
            tp_rate *= table.loc['tp', str(v)] / table.sum(axis=1)['tp']
            fp_rate *= table.loc['fp', str(v)] / table.sum(axis=1)['fp']
        cfd = (tp_rate * tp/(tp + fp), fp_rate *fp/(tp + fp))
        cfd = cfd[0]/(cfd[0] + cfd[1])
        cfds[typee] = (url, reorg_url, cfd)
    if not r_feature:
        return cfds
    else:
        return cfds, f