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


def is_updating():
    """
    Get all urls from utl_update labeled as "similar?"
    Compute pairwise similarities.
    if any of the pair match. Comsider as similar, otherwise, not similar
    """
    corpus = db.url_content.find({"usage": re.compile("updating")}, {"content": True})
    corpus = [c['content'] for c in corpus]
    print("Got content", len(corpus))
    tfidf = text_utils.TFidf(corpus)
    print("tfidf init success")
    rvals = db.url_update.aggregate([
        {"$match": {"detail": 'not similar?'}},
        {"$lookup": {
            "from": "url_content",
            "let": {"id": "$_id"},
            "pipeline": [
                {"$match": {"usage": re.compile("updating")}},
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$$id", "$url"]}
                ]}}} 
            ],
            "as": "contents"
        }},
        {"$project": {"contents.content": True, "contents.ts": True}}
    ])
    rvals = list(rvals)
    print('total:', len(rvals))
    for i, similarity in enumerate(rvals):
        if i % 1000 == 0: print(i)
        max_simi = [(0, ("00000000000000", "00000000000000"))]
        contents = similarity['contents']
        for content1, content2 in itertools.combinations(contents, 2):
            simi = tfidf.similar(content1['content'], content2['content'])
            max_simi.append((simi, (content1['ts'], content2['ts'])))
        max_simi = max(max_simi, key=lambda x: x[0])
        updating = True if max_simi[0] < 0.8 else False
        detail = 'similar' if not updating else "not similar"
        db.url_update.update_one({"_id": similarity['_id']}, {"$set": \
            {"similarity": max_simi[0], "recordts": list(max_simi[1]), "updating": updating, "detail": detail }})


is_updating()