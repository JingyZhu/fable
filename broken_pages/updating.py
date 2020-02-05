"""
Study between the relation of uipdating urls vs. Unsure rate
"""
import requests
import sys
from pymongo import MongoClient
import pymongo
import json, yaml
import os
import queue, threading
import brotli
import random
import re, time
import itertools, collections

sys.path.append('../')
from utils import text_utils, crawl
import config

PS = crawl.ProxySelector(config.PROXIES)
db = MongoClient(config.MONGO_HOSTNAME).web_decay


def domsitiller_vs_boilerpipe():
    """
    Compare the content extracted by domsitiller and boilerpipe
    Turns out domdistiller has more accurate content to generate
    """
    samples = db.url_content.aggregate([
        {"$sample": {"size": 100}},
        {"$project": {"_id": False}}
    ])
    domdistiller, boilerpipe = [], []
    for sample in samples:
        if sample['src'] == 'wayback':
            sample['url'] = 'https://web.archive.org/web/{}/{}'.format(sample['ts'], sample['url'])
        html = brotli.decompress(sample['html']).decode()
        del(sample['html'])
        domdistiller.append(sample.copy())
        content = text_utils.extract_body(html, version='boilerpipe')
        sample['content'] = content
        boilerpipe.append(sample)
    domdistiller.sort(key=lambda x: x['url'])
    boilerpipe.sort(key=lambda x: x['url'])
    json.dump(domdistiller, open('../tmp/dom.json', 'w+'))
    json.dump(boilerpipe, open('../tmp/boiler.json', 'w+'))

domsitiller_vs_boilerpipe()