"""
Use searched url to infer potentially more urls not indexed by google
"""
import requests
import sys
from pymongo import MongoClient
import pymongo
import json
import os
import brotli
import socket
import random
import re, time
import itertools, collections

sys.path.append('../')
from utils import url_utils
import config

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay


def generate_inferred_rules():
    db.search_infer_guess.create_index([("url", pymongo.ASCENDING), ("from", pymongo.ASCENDING)], unique=True)
    urls = db.search_infer_meta.find({"similarity": {"$gte": 0.8}})
    site_dict = collections.defaultdict(int)
    he = url_utils.HostExtractor()

    for url in urls:
        orig_site = he.extract(url['url'])
        searched_site = he.extract(url['searched_url'])
        if orig_site == searched_site: site_dict[orig_site] += 1

    searched_urls = db.search_infer_meta.aggregate([
        {"$match": {"usage": "represent"}},
        {"$lookup": {
            "from": "url_broken",
            "localField": "url",
            "foreignField": "_id",
            "as": "stat"
        }},
        {"$unwind": "$stat"},
        {"$group": {"_id": {"hostname": "$stat.hostname", "year": "$stat.year"}, "urls": {"$push": {"url": "$url", "searched_url": "$searched_url", "similarity": "$similarity"}}}}
    ])
    URI = url_utils.UrlRuleInferer()
    searched_urls = list(filter(lambda x: x['_id']['hostname'] in site_dict, list(searched_urls)))
    for site_urls in searched_urls:
        print('Site:', site_urls['_id']['hostname'])
        if_objs = []
        site_searched = []       
        for url in site_urls['urls']:
            if url['similarity'] >= 0.8:
                site_searched.append((url['url'],url['searched_url']))
        URI.learn_rules(site_searched, site_urls['_id']['hostname'])
        for url in site_urls['urls']:
            if url['similarity'] < 0.8:
                inferred_urls = URI.infer(url['url'])
                print('url:', url['url'], inferred_urls)
                if_objs += [{
                    "url": url['url'],
                    "from": iu
                } for iu in inferred_urls]
        try: db.search_infer_guess.insert_many(if_objs, ordered=False)
        except: pass

generate_inferred_rules()