"""
Load broken page on the wayback machine
Extract page metadata (title, topN words, etc)
Search on google

Pipeline: crawl_wayback_wrapper --> calculate_topN --> search_titleMatch_topN
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
    elif len(cps) <= 0: return [], {}
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
    


def crawl_wayback(q_in, tid):
    """
    crawl broken pages on wayback machine. Store in db.search
    Extract title from html
    """
    global counter
    wm_objs = []
    while not q_in.empty():
        url, year = q_in.get()
        wayback_cp, usage = get_wayback_cp(url, year)
        title = ""
        for ts, html, content in wayback_cp:
            if usage[ts] == 'represent':
                title = search.get_title(html)
            wm_objs.append({
                "url": url,
                "ts": ts,
                "html": brotli.compress(html.encode()),
                "content": content,
                "titleMatch": title,
                "usage": usage[ts]
            })
        counter += 1
        print(counter, tid, url, len(list(filter(lambda x: x[2] != "", wayback_cp))))
        if len(wm_objs) >= 30:
            try: db.search_meta.insert_many(wm_objs, ordered=False)
            except: pass
            wm_objs = []
        
    try: db.search_meta.insert_many(wm_objs, ordered=False)
    except: pass


def crawl_wayback_wrapper(NUM_THREADS=5):
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
        pools.append(threading.Thread(target=crawl_wayback, args=(q_in, i)))
        pools[-1].start()
    for t in pools:
        t.join()


def search_titleMatch_topN():
    urls = db.search_meta.aggregate([
        {"$match": {"usage": "represent"}},
        {"$lookup":{
            "from": "searched_titleMatch",
            "localField": "url",
            "foreignField": "_id",
            "as": "hasSearched"
        }},
        {"$match": {"hasSearched.0": {"$exists": False}}},
        {"$project": {"hasSearched": False}}
    ])
    se_objs = []
    urls = list(urls)
    print(len(urls))
    for i, obj in enumerate(urls):
        titleMatch, topN, url = obj['titleMatch'], obj.get('topN'), obj['url']
        db.searched_titleMatch.insert_one({"_id": url})
        db.searched_topN.insert_one({"_id": url})
        if titleMatch:
            search_results = search.google_search('"{}"'.format(titleMatch))
            if search_results is None:
                print("No more access to google api")
                break
            print(i, len(search_results), url, titleMatch)
            for j, search_url in enumerate(search_results):
                se_objs.append({
                    "url": search_url,
                    "from": url,
                    "rank": "top5" if j < 5 else "top10"
                })
        if topN:
            search_results = search.google_search(topN)
            if search_results is None:
                print("No more access to google api")
                break
            print(i, len(search_results), url, topN)
            for j, search_url in enumerate(search_results):
                se_objs.append({
                    "url": search_url,
                    "from": url,
                    "rank": "top5" if j < 5 else "top10"
                })
        if len(se_objs) >= 50:
            try: db.search.insert_many(se_objs, ordered=False)
            except: pass
            se_objs = []

    try: db.search.insert_many(se_objs, ordered=False)
    except: pass


def calculate_topN():
    corpus = db.search_meta.find({'content': {"$ne": ""}}, {'content': True})
    corpus = [c['content'] for c in corpus]
    tfidf = text_utils.TFidf(corpus)
    print('tfidf initialized')
    urls = list(db.search_meta.find({'content': {"$ne": ""}, 'topN': {"$exists": False}}))
    for i, url in enumerate(urls):
        words = tfidf.topN(url['content'])
        query = ' '.join(words)
        db.search_meta.update_one({"url": url['url'], "ts": url['ts']}, {"$set": {"topN": query}})
        if i % 100 == 0: print(i)


if __name__ == '__main__':
    search_titleMatch_topN()