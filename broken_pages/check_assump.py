"""
Script used to check the assumptions
"""
import sys
from pymongo import MongoClient
import json
import os
import queue, threading
import brotli
import socket
import random
import re
import itertools

sys.path.append('../')
from utils import text_utils, crawl, url_utils, plot
import config

idx = config.HOSTS.index(socket.gethostname())
PS = crawl.ProxySelector(config.PROXIES)
db = MongoClient(config.MONGO_HOSTNAME).web_decay
counter = 0

def get_wayback_contents(url):
    """
    Get multiple wayback's copies in each year if possible.
    Pick 3 of them, and pick the most likely good one

    Return wayback content as in different cp. If no content, content = ''
    If HLD, return None
    """
    param_dict = {
        "filter": ['statuscode:200', 'mimetype:text/html'],
        "limit": 50,
        "collapse": "timestamp:8",
    }
    cps, _ = crawl.wayback_index(url, param_dict=param_dict, total_link=True, proxies=PS.select())
    if len(cps) > 3: cps = random.sample(cps, 3)
    contents = []
    tss = []
    for ts, cp in cps:
        html = crawl.requests_crawl(cp, proxies=PS.select())
        if not html: continue
        if url_utils.find_link_density(html) >= 0.8: return None
        content = text_utils.extract_body(html, version='domdistiller')
        contents.append(content)
        tss.append(ts)
    return contents, tss


def unsure_fraction(NUM_THREADS=5):
    """
    For 200/300 urls, check if the unsure frac of High link density / Updating among multiple snapshots
    is very different with the others

    Query the wayback CDX to get random 3 snapshots (if availalbe, different days), compare pairwise simi
    If there is no pair similar, consider not-similar

    Output the results into collection: url_update
    """
    counter = 0
    corpus = db.url_content.aggregate([
        {"$match": {"content": {"$exists": True, "$ne": ""}}},
        {"$sample": {"size": 10000}},
        {"$project": {"content": True}}
    ])
    corpus = [c['content'] for c in corpus]
    tfidf = text_utils.TFidf(corpus)
    q_in = queue.Queue()
    def thread_func(q_in, tid):
        nonlocal counter, corpus
        while not q_in.empty():
            url = q_in.get()
            counter += 1
            print(counter, tid, url)
            detail, updating = "", None
            contents, ts = get_wayback_contents(url)
            if contents is None:
                detail = 'HLD'
                updating = True
            elif len(contents) == 0:
                detail = 'no html'
                updating = False
            elif len(contents) == 1:
                updating = False
                if contents[0] == "": detail = "no content"
                else: detail = "1 snapshot"
            elif len(list(filter(lambda x: x != "", contents))) == 0:
                updating = True
                detail = "no contents"
            else:
                updating = True
                detail = "not similar"
                for c1, c2 in itertools.combinations(contents, 2):
                    if tfidf.similar(c1, c2) >= .8:
                        updating = False
                        detail = "similar"
                        break
            try:
                db.url_update.insert_one({
                    "_id": url,
                    "url": url,
                    "updating": updating,
                    "ts": ts,
                    "detail": detail
                })
            except: pass

    urls = db.url_status_implicit_broken.aggregate([
        {"$match": {"status": re.compile("^[23]")}},
        {"$lookup": {
            "from": "url_content",
            "localField": "_id",
            "foreignField": "url",
            "as": "contents"
        }},
        {"$unwind": "$contents"},
        {"$replaceRoot": { "newRoot": "$contents"} },
        {"$project": {"url": True}}
    ])
    urls = list(urls)
    random.shuffle(urls)
    print(len(urls))
    for url in urls:
        q_in.put(url['url'])
    pools = []
    for i in range(NUM_THREADS):
        pools.append(threading.Thread(target=thread_func, args=(q_in, i)))
        pools[-1].start()
    for t in pools:
        t.join()

unsure_fraction()


