"""
Load broken page on the wayback machine
Extract page metadata (title, topN words, etc)
Search on google

Pipeline: crawl_wayback_wrapper --> calculate_topN --> search_titleMatch_topN --> crawl_realweb_wrapper --> calculate_similarity
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
import multiprocessing as mp
import signal

sys.path.append('../')
from utils import text_utils, crawl, url_utils, search
import config

idx = config.HOSTS.index(socket.gethostname())
PS = crawl.ProxySelector(config.PROXIES)
db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
db_test = MongoClient(config.MONGO_HOSTNAME).wd_test
counter = mp.Value('i', 0)
host_extractor = url_utils.HostExtractor()

def timeout_handler(signum, frame):
    print(mp.current_process().name)
    raise Exception("Timeout")


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
    for ts, cp, _ in cps:
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
            try: db.search_infer_meta.insert_many(wm_objs, ordered=False)
            except: pass
            wm_objs = []
        
    try: db.search_infer_meta.insert_many(wm_objs, ordered=False)
    except: pass


def crawl_wayback_wrapper(NUM_THREADS=5):
    db.search_infer_meta.create_index([("url", pymongo.ASCENDING), ("ts", pymongo.ASCENDING)], unique=True)
    db.search_infer.create_index([("url", pymongo.ASCENDING), ("from", pymongo.ASCENDING)], unique=True)
    q_in = queue.Queue()
    urls = db.url_broken.aggregate([
        {"$lookup": {
            "from": "search_infer_meta",
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


def crawl_realweb(q_in, tid, counter):
    se_ops = []
    db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
    while not q_in.empty():
        url, fromm = q_in.get()
        with counter.get_lock():
            counter.value += 1
            print(counter.value, tid, url)
        html = crawl.requests_crawl(url)
        if html is None: html = ''
        content = text_utils.extract_body(html, version='domdistiller')
        se_ops.append(pymongo.UpdateOne(
            {"url": url, "from": fromm}, 
            {"$set": {"html": brotli.compress(html.encode()), "content": content}}
        ))
        if len(se_ops) >= 1:
            try: db.search_infer.bulk_write(se_ops)
            except: print("db bulk write failed")
            se_ops = []
    try: db.search_infer.bulk_write(se_ops)
    except: print("db bulk write failed")


def crawl_realweb_wrapper(NUM_THREADS=10):
    """
    Crawl the searched results from the db.search
    Update each record with html (byte) and content
    """
    counter = mp.Value('i', 0)
    q_in = mp.Queue()
    urls = db.search_infer.find({'html': {"$exists": False}})
    urls = sorted(list(urls), key=lambda x: x['url'] + str(x['from']))
    length = len(urls)
    print(length // len(config.HOSTS))
    urls = urls[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    random.shuffle(urls)
    for url in urls:
        q_in.put((url['url'], url['from']))
    pools = []
    for i in range(NUM_THREADS):
        pools.append(mp.Process(target=crawl_realweb, args=(q_in, i, counter)))
        pools[-1].start()
    def segfault_handler(signum, frame):
        print("Seg Fault on process")
        if not q_in.empty():
            pools.append(mp.Process(target=crawl_realweb, args=(q_in, len(pools), counter)))
            pools[-1].start()
            pools[-1].join()
    signal.signal(signal.SIGSEGV, segfault_handler) 
    for t in pools:
        t.join()


def search_titleMatch_topN():
    urls = db.search_infer_meta.aggregate([
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
    random.shuffle(urls)
    urls = urls[:4900]
    print('total:', len(urls))
    for i, obj in enumerate(urls):
        titleMatch, url = obj['titleMatch'], obj['url']
        if titleMatch:
            search_results = search.google_search('"{}"'.format(titleMatch), use_db=True)
            if search_results is None:
                print("No more access to google api")
                break
            db.searched_titleMatch.insert_one({'_id': url})
            print(i, len(search_results), url, titleMatch)
            for j, search_url in enumerate(search_results):
                se_objs.append({
                    "url": search_url,
                    "from": url,
                    "rank": "top5" if j < 5 else "top10"
                })
        if len(se_objs) >= 10:
            try: db.search_infer.insert_many(se_objs, ordered=False)
            except: pass
            se_objs = []
    try: db.search_infer.insert_many(se_objs, ordered=False)
    except: pass
    
    urls = db.search_infer_meta.aggregate([
        {"$match": {"usage": "represent"}},
        {"$lookup":{
            "from": "searched_topN",
            "localField": "url",
            "foreignField": "_id",
            "as": "hasSearched"
        }},
        {"$match": {"hasSearched.0": {"$exists": False}}},
        {"$project": {"hasSearched": False}}
    ])
    se_objs = []
    urls = list(urls)
    random.shuffle(urls)
    urls = urls[:4900]
    print('total:', len(urls))
    for i, obj in enumerate(urls):
        topN, url = obj.get('topN'), obj['url']
        if topN:
            print(i, url, topN)
            search_results = search.google_search(topN, site_spec_url=obj['url'], use_db=True)
            if search_results is None:
                print("No more access to google api")
                break
            db.searched_topN.insert_one({'_id': url})
            print(i, len(search_results), url, topN)
            for j, search_url in enumerate(search_results):
                se_objs.append({
                    "url": search_url,
                    "from": url,
                    "rank": "top5" if j < 5 else "top10"
                })
        if len(se_objs) >= 10:
            try: db.search_infer.insert_many(se_objs, ordered=False)
            except: pass
            se_objs = []
    try: db.search_infer.insert_many(se_objs, ordered=False)
    except: pass


def calculate_titleMatch(NUM_THREADS=10):
    urls = list(db.search_infer_meta.find({'titleMatch': {"$exists": False}}))
    print('total:', len(urls))
    q_in = mp.Queue()
    count = mp.Value('i', 0)
    def proc_func(q_in, pid):
        nonlocal count
        db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
        while not q_in.empty():
            with count.get_lock():
                count.value += 1
                print(count.value, pid)
            url, ts, html = q_in.get()
            title = search.get_title(html)
            db.search_infer_meta.update_one({"url": url, "ts": ts}, {"$set": {"titleMatch": title}})
    for url in urls:
        try: html = brotli.decompress(url['html']).decode()
        except:
            print("fail to decode")
            continue
        q_in.put((url['url'], url['ts'], html))
    pools = []
    print(os.getpid())
    for i in range(NUM_THREADS):
        pools.append(mp.Process(target=proc_func, args=(q_in, i)))
        pools[-1].start()
    for p in pools:
        p.join()


def calculate_topN():
    corpus = db.search_infer_meta.find({}, {'content': True})
    corpus = [c['content'] for c in corpus]
    tfidf = text_utils.TFidf(corpus)
    print('tfidf initialized')
    urls = list(db.search_infer_meta.find({'topN': {"$exists": False}}))
    print('total:', len(urls))
    for i, url in enumerate(urls):
        words = tfidf.topN(url['content'])
        query = ' '.join(words)
        db.search_infer_meta.update_one({"url": url['url'], "ts": url['ts']}, {"$set": {"topN": query}})
        if i % 100 == 0: print(i)


def calculate_similarity():
    """
    Calcuate the (highest) similarity of each searched pages
    Update similarity and searched_urls to db.search_infer_meta
    """
    corpus1 = db.search_infer_meta.find({'content': {"$ne": ""}}, {'content': True})
    corpus2 = db.search_infer.find({'content': {"$exists": True,"$ne": ""}}, {'content': True})
    corpus = [c['content'] for c in corpus1] + [c['content'] for c in corpus2]
    tfidf = text_utils.TFidf(corpus)
    print("tfidf init success!")
    searched_urls = db.search_infer_meta.aggregate([
        {"$match": {"usage": "represent", "similarity": {"$exists": False}}},
        {"$lookup": {
            "from": "search_infer",
            "localField": "url",
            "foreignField": "from",
            "as": "searched"
        }},
        {"$project": {"searched.html": False, "searched._id": False, "_id": False, "html": False}},
        {"$unwind": "$searched"},
        {"$match": {"searched.content": {"$exists": True, "$ne": ""}}}
    ])
    searched_urls = list(searched_urls)
    simi_dict = collections.defaultdict(lambda: {'ts': 0, 'simi': 0, 'searched_url': ''})
    print('total comparison:', len(searched_urls))
    for i, searched_url in enumerate(searched_urls):
        if i % 100 == 0: print(i)
        url, ts, content = searched_url['url'], searched_url['ts'], searched_url['content']
        searched = searched_url['searched']
        simi = tfidf.similar(content, searched['content'])
        simi_dict[url]['ts'] = ts
        if simi >= simi_dict[url]['simi']:
            simi_dict[url]['simi'] = simi
            simi_dict[url]['searched_url'] = searched['url']
    search_infer_meta = db.search_infer_meta.find({"usage": "represent", "similarity": {"$exists": False}}, {'url': True, 'ts': True})
    for obj in list(search_infer_meta):
        url, ts = obj['url'], obj['ts']
        value = simi_dict[url]    
        db.search_infer_meta.update_one({'url': url, 'ts': ts}, \
            {'$set': {'similarity': value['simi'], 'searched_url': value['searched_url']}})


if __name__ == '__main__':
    calculate_similarity()