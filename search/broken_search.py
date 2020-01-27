"""
Load broken page on the wayback machine
Extract page metadata (title, topN words, etc)
Search on google
"""
import requests
import sys
from pymongo import MongoClient
import pymongo
import json
import os
import queue, threading
import brotli
import socket
import random
import re, time
import itertools, collections

sys.path.append('../')
from utils import text_utils, crawl, url_utils, search
import config

idx = config.HOSTS.index(socket.gethostname())
PS = crawl.ProxySelector(config.PROXIES)
db = MongoClient(config.MONGO_HOSTNAME).web_decay
counter = 0

def get_wayback_cp(url, year):
    """
    Get most 3 available pages on wayback
    Load pages return (ts, html, content), functions for different html (archive / represent)
    """
    param_dict = {
        "from": str(year-1) + '0101',
        "to": str(year+1) + '1231',
        "filter": ['statuscode:200', 'mimetype:text/html'],
        "limit": 100
    }
    cps, _ = crawl.wayback_index(url, param_dict=param_dict, total_link=True, proxies=PS.select())
    if len(cps) > 3: cps = random.sample(cps, 3)
    wayback = []
    usage = {u[0]: 'archive' for u in cps}
    for ts, cp in cps:
        html = crawl.requests_crawl(cp, proxies=PS.select())
        if not html: continue
        content = text_utils.extract_body(html, version='domdistiller')
        wayback.append((ts, html, content))
    represent = None if len(wayback) == 0 else max(wayback, key=lambda x: len(x[2].split()))
    if represent: usage[represent[0]] = 'represent'
    else: usage[random.choice(list(usage.keys()))] = 'represent'
    return wayback, usage
    


def crawl_and_titlesearch(q_in, tid):
    """
    crawl broken pages on wayback machine. Store in db.search
    Extract title from html, do a google search on title. Store results (not crawled content yet) in db.metadata_search
    """
    global counter
    se_objs = []
    wm_objs = []
    while not q_in.empty():
        url, year = q_in.get()
        wayback_cp, usage = get_wayback_cp(url, year)
        counter += 1
        print(counter, tid, url)
        for ts, html, content in wayback_cp:
            title = search.get_title(html)
            wm_objs.append({
                "url": url,
                "ts": ts,
                "html": brotli.compress(html.encode()),
                "content": content,
                "titleMatch": title,
                "usage": usage[ts]
            })
            search_results = search.google_search('"{}"'.format(title))
            for i, search_url in enumerate(search_results):
                se_objs.append({
                    "url": search_url,
                    "from": url,
                    "rank": "top5" if i < 5 else "top10"
                })
        if len(wm_objs) >= 50:
            try: db.search_meta.insert_many(wm_objs, ordered=False)
            except: pass
            try: db.search.insert_many(se_objs, ordered=False)
            except: pass
            wm_objs, se_objs = [], []
        
        try: db.search_meta.insert_many(wm_objs, ordered=False)
        except: pass
        try: db.search.insert_many(se_objs, ordered=False)
        except: pass


def crawl_and_titlesearch_wrapper(NUM_THREADS=10):
    db.search_meta.create_index([("url", pymongo.ASCENDING), ("ts", pymongo.ASCENDING)], unique=True)
    db.search.create_index([("url", pymongo.ASCENDING), ("from", pymongo.ASCENDING)], unique=True)
    q_in = queue.Queue()
    urls = db.url_broken.aggregate([
        {"$lookup": {
            "from": "search_meta",
            "localField": "_id",
            "foreignField": "url",
            "as": "broken"
        }},
        {"$match": {"broken.0": {"$exists": False}}}
    ])
    urls = list(urls)
    urls = sorted(list(urls), key=lambda x: x['_id'] + str(x['year']))
    length = len(urls)
    print(length // len(config.HOSTS))
    urls = urls[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    
    random.shuffle(urls)
    for url in urls:
        q_in.put((url['url'], url['year']))
    pools = []
    for i in range(NUM_THREADS):
        pools.append(threading.Thread(target=crawl_and_titlesearch, args=(q_in, i)))
        pools[-1].start()
    for t in pools:
        t.join()


if __name__ == '__main__':
    crawl_and_titlesearch_wrapper()