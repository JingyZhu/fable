"""
Use searched url to infer potentially more urls not indexed by google
"""
import requests
import sys
from pymongo import MongoClient
import pymongo
import json
import os, signal
import brotli
import socket
import random
import re, time
import requests
import itertools, collections
import multiprocessing as mp

sys.path.append('../')
from utils import url_utils, text_utils, crawl
import config

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
requests_header = {'user-agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"}


def generate_inferred_rules(NUM_PROC=16):
    """Entry Func. Generate inferred urls from searched urls for not-searched urls"""
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
    searched_urls = list(filter(lambda x: x['_id']['hostname'] in site_dict, list(searched_urls)))
    def proc_func(q_in, pid):
        URI = url_utils.UrlRuleInferer()
        db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
        while not q_in.empty():
            site_urls = q_in.get()
            print(pid, 'Site:', site_urls['_id']['hostname'])
            if_objs = []
            site_searched = []       
            for url in site_urls['urls']:
                if url['similarity'] >= 0.8:
                    site_searched.append((url['url'],url['searched_url']))
            URI.learn_rules(site_searched, site_urls['_id']['hostname'])
            for url in site_urls['urls']:
                if url['similarity'] < 0.8:
                    inferred_urls = URI.infer(url['url'])
                    print(pid, 'url:', url['url'], inferred_urls)
                    if_objs += [{
                        "url": 'http://' + iu,
                        "from": url['url']
                    } for iu in inferred_urls]
            try: db.search_infer_guess.insert_many(if_objs, ordered=False)
            except: pass
            print(pid, "Finished one site")
    q_in = mp.Queue()
    for site_urls in searched_urls:
        q_in.put(site_urls)
    pools = []
    for i in range(NUM_PROC):
        pools.append(mp.Process(target=proc_func, args=(q_in, i,)))
        pools[-1].start()
    for p in pools:
        p.join()


def crawl_guess(q_in, pid, counter):
    if_ops = []
    db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
    while not q_in.empty():
        url, fromm = q_in.get()
        update_dict = {}
        with counter.get_lock():
            counter.value += 1
            print(counter.value, pid, url)
        r = requests.get(url, headers=requests_header, timeout=15)
        update_dict['status'] = str(r.status_code)
        if r.status_code < 400:
            html = crawl.requests_crawl(url)
            if html is None: html = ''
            content = text_utils.extract_body(html, version='domdistiller')
            update_dict.update({'html': brotli.compress(html.encode()), 'content': content})
        if_ops.append(pymongo.UpdateOne(
            {"url": url, "from": fromm}, 
            {"$set": update_dict}
        ))
        if len(if_ops) >= 1:
            try: db.search_infer_guess.bulk_write(if_ops)
            except: print("db bulk write failed")
            if_ops = []
    try: db.search_infer_guess.bulk_write(if_ops)
    except: print("db bulk write failed")


def crawl_guess_wrapper(NUM_PROCS=10):
    """
    Entry Func
    Crawl the guess results from the db.search_infer_guess
    Update each record with html (byte) and content if the status code is 2xx
    """
    counter = mp.Value('i', 0)
    q_in = mp.Queue()
    urls = db.search_infer_guess.find({'status': {"$exists": False}})
    urls = list(urls)
    print(len(urls))
    random.shuffle(urls)
    for url in urls:
        q_in.put((url['url'], url['from']))
    pools = []
    for i in range(NUM_PROCS):
        pools.append(mp.Process(target=crawl_guess, args=(q_in, i, counter)))
        pools[-1].start()
    def segfault_handler(signum, frame):
        print("Seg Fault on process")
        if not q_in.empty():
            pools.append(mp.Process(target=crawl_guess, args=(q_in, len(pools), counter)))
            pools[-1].start()
            pools[-1].join()
    signal.signal(signal.SIGSEGV, segfault_handler) 
    for t in pools:
        t.join()


if __name__ == '__main__':
    generate_inferred_rules()