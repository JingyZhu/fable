"""
Unlike status code checking. Page could even broken at the status code of 2xx/3xx
This scipt is used for checking 2/3xx status code
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

sys.path.append('../')
from utils import text_utils, crawl, url_utils, plot
import config

idx = config.HOSTS.index(socket.gethostname())
PS = crawl.ProxySelector(config.PROXIES)
db = MongoClient(config.MONGO_HOSTNAME).web_decay
counter = mp.Value('i', 0)


def decide_content(html):
    """
    Decide whether the page is a navigational page
    If it is, return content=empty
    Else, return the most confident content
    """
    if url_utils.find_link_density(html) >= 0.8: return ""
    return text_utils.extract_body(html, version='domdistiller')


def get_wayback_cp(url, year):
    """
    Get multiple wayback's copies in each year if possible.
    Pick 3 of them, and pick the most likely good one

    If there is a best fit, return (ts, html, content)
    Else return None

    return: data in certain year, data in all years, each data function (updating, represent ... )
    """
    param_dict = {
        "filter": ['statuscode:200', 'mimetype:text/html'],
        "collapse": "timestamp:8",
        "limit": 100
    }
    cps, _ = crawl.wayback_index(url, param_dict=param_dict, total_link=True, proxies=PS.select())
    cp_in_year = list(filter(lambda x: abs(int(str(x[0])[:4]) - year) <= 1, cps))
    # Updating sample
    cp_sample = random.sample(cps, 3) if len(cps) > 3 else cps
    # Overlap part of two samples
    cp_in_year_sample = [c for c in cp_sample if abs(int(str(c[0])[:4]) - year) <= 1]
    if len(cp_in_year_sample) < 3 and len(cp_in_year_sample) < len(cp_in_year):
        sample = [c for c in cp_in_year if c not in set(cp_in_year_sample)]
        size = min(3 - len(cp_in_year_sample), len(cp_in_year) - len(cp_in_year_sample))
        cp_in_year_sample = cp_in_year_sample + random.sample(sample, size)
    wayback_year = []
    updating = []
    # print([t[0] for t in cp_in_year_sample], [t[0] for t in cp_sample])
    cp_in_year_dict = {c[0]: None for c in cp_in_year_sample} # ts: (ts, html, content)
    functions = collections.defaultdict(list)
    for ts, cp in cp_sample:
        html = crawl.requests_crawl(cp, proxies=PS.select())
        if not html: continue
        if url_utils.find_link_density(html) >= 0.8:
            updating = None
            if ts in cp_in_year_dict:
                content = text_utils.extract_body(html, version='domdistiller')
                cp_in_year_dict[ts] = (ts, html, content)
            break
        content = text_utils.extract_body(html, version='domdistiller')
        cp_in_year_dict[ts] = (ts, html, content)
        updating.append((ts, html, content))
        functions[ts].append('updating')

    for ts, cp in cp_in_year_sample:
        if cp_in_year_dict[ts] is not None:
            ts, html, content = cp_in_year_dict[ts]
        else:
            html = crawl.requests_crawl(cp, proxies=PS.select(), wait=True)
            if not html: continue
            content = decide_content(html)
        if content == '': continue
        wayback_year.append((ts, html, content))
    represent = None if len(wayback_year) == 0 else max(wayback_year, key=lambda x: len(x[2].split()))
    if represent: functions[represent[0]].append('represent')
    for w in wayback_year:
        if w[0] != represent[0]: functions[represent[0]].append('archive')
    return wayback_year, updating, functions


def crawl_pages(q_in, tid):
    """
    Get content from both wayback and realweb
    Update content into db.url_content
    Get different cps see if the page is updating frequently
    Add into db.url_update
    """
    global counter
    db = MongoClient(config.MONGO_HOSTNAME).web_decay
    rw_objs, wm_objs, uu_objs, wu_objs = [], [], [], []
    while not q_in.empty():
        url, year = q_in.get()
        wayback_year, wayback_update, functions = get_wayback_cp(url, year)
        with counter.get_lock():
            counter.value += 1
            print(counter.value, tid, url, len(wayback_year), len(wayback_update) if wayback_update else 0)
        if len(wayback_year): # Possibliy not landing pages
            rw_html = crawl.requests_crawl(url)
            if not rw_html: rw_html = ''
            rw_content = decide_content(rw_html)
            rw_objs.append({
                "url": url,
                "src": "realweb",
                "ts": 20200126000000,
                "html": brotli.compress(rw_html.encode()),
                "content": rw_content
            })
            for ts, html, content in wayback_year:
                wm_objs.append({
                    "url": url,
                    "src": "wayback",
                    "ts": ts,
                    "html": brotli.compress(html.encode()),
                    "content": content,
                    "usage": ' '.join(sorted(functions[ts]))
                })
            if len(rw_objs) >= 50 or len(wm_objs) >= 50:
                try: db.url_content.insert_many(rw_objs, ordered=False)
                except: print(tid, "db operation failed")
                try: db.url_content.insert_many(wm_objs, ordered=False)
                except: print(tid, "db operation failed")
                rw_objs, wm_objs = [], []
        if wayback_update is None:
                detail = 'HLD'
                updating = True
        elif len(wayback_update) == 0:
            detail = 'no html'
            updating = False
        elif len(wayback_update) == 1:
            updating = False
            if wayback_update[0][2] == "": detail = "no content"
            else: detail = "1 snapshot"
        elif len(list(filter(lambda x: x[2] != "", wayback_update))) == 0:
            updating = True
            detail = "no contents"
        else:
            updating = True
            detail = "not similar?"
        if not wayback_update: wayback_update = []
        tss = [u[0] for u in wayback_update]
        uu_obj = {
            "_id": url,
            "url": url,
            "updating": updating,
            "tss": tss,
            "detail": detail
        }
        uu_objs.append(uu_obj)
        for ts, html, content in wayback_update:
            if len(functions[ts]) > 1 or content == "": continue
            wu_objs.append({
                "url": url,
                "src": "wayback",
                "ts": ts,
                "html": brotli.compress(html.encode()),
                "content": content,
                "usage": functions[ts][0]
            })
        if len(uu_objs) >= 50:
            try: db.url_update.insert_many(uu_objs, ordered=False)
            except: print(tid, "db operation failed")
            uu_objs = []
        if len(wu_objs) >= 50:
            try: db.url_content.insert_many(wu_objs, ordered=False)
            except: print(tid, "db operation failed")
            wu_objs = []
    try: db.url_content.insert_many(rw_objs, ordered=False)
    except: pass
    try: db.url_content.insert_many(wm_objs, ordered=False)
    except: pass
    try: db.url_update.insert_many(uu_objs, ordered=False)
    except: pass
    try: db.url_content.insert_many(wu_objs, ordered=False)
    except: pass


def crawl_pages_wrap(NUM_THREADS=5):
    """
    Get sampled hosts from db.host_sample
    Get all 2xx/3xx urls from sampled hosts
    """
    db.url_content.create_index([("url", pymongo.ASCENDING), ("ts", pymongo.ASCENDING)], unique=True)
    db.url_content.create_index([("url", pymongo.ASCENDING)])
    q_in = mp.Queue()
    # Get content of url_status join host_sample, which is not in url_content 
    urls = db.host_sample.aggregate([
        {"$lookup": {
            "from": "url_status",
            "let": {"hostname": "$hostname", "year": "$year"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$hostname", "$$hostname"]},
                    {"$eq": ["$year", "$$year"]}
                ]}}}
            ],
            "as": "in_sample"
        }},
        {"$match": {"in_sample.0": {"$exists": True}}},
        {"$unwind": "$in_sample"},
        {"$replaceRoot": { "newRoot": "$in_sample"} },
        {"$match": {"status": re.compile("^[23]")}},
        # {"$count": "count"}
        {"$lookup": {
            "from": "url_update",
            "localField": "_id",
            "foreignField": "_id",
            "as": "updates"
        }},
        {"$match": {"updates.0": {"$exists": False}}},
    ])
    urls = list(urls)
    urls = sorted(list(urls), key=lambda x: x['_id'] + str(x['year']))
    length = len(urls)
    print(length // len(config.HOSTS))
    urls = urls[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    
    random.shuffle(urls)
    # urls = [('https://cnn.com', 2019)]
    for url in urls:
        q_in.put((url['url'], url['year']))
    pools = []
    for i in range(NUM_THREADS):
        pools.append(mp.Process(target=crawl_pages, args=(q_in, i)))
        pools[-1].start()
    for t in pools:
        t.join()


def host_sampling():
    db.host_sample.create_index([("hostname", pymongo.ASCENDING), ("year", pymongo.ASCENDING)], unique=True)
    for year in [1999, 2004, 2009, 2014, 2019]:
        hosts = db.host_status.aggregate([
            {"$match": {"year": year, "status": re.compile("^[23]")}},
            {"$group": {"_id": "$hostname"}},
            {"$project": {"hostname": "$_id", "_id": False, "year": {"$literal": year}}}
        ])
        hosts = list(hosts)
        hosts = random.sample(hosts, 1000)
        db.host_sample.insert_many(hosts, ordered=False)


def compute_broken():
    """
    Compute the TFidf similarity of urls with existed content
    Update similarities to url_status
    """
    contents = db.url_content.find({}, {'content': True})
    contents = [c['content'] for c in contents]
    print("Got contents")
    tfidf = text_utils.TFidf(contents)
    print("tdidf init success!")
    available_urls = db.url_status_implicit_broken.aggregate([
        {"$match": {"status": re.compile('^[23]') }},
        {"$lookup": {
            "from": "url_content",
            "localField": "_id",
            "foreignField": "url",
            "as": "contents"
        }},
        {"$match": {"contents.0": {"$exists": 1}}},
        {"$project": {"contents.html": False, "contents._id": False}}
    ])
    available_urls = list(available_urls)
    print("Total:", len(available_urls))
    similarities = []
    for i, url in enumerate(available_urls):
        content = url['contents']
        simi = tfidf.similar(content[0]['content'], content[1]['content'])
        similarities.append(simi)
        db.url_status_implicit_broken.update_one({"_id": url['_id']}, {"$set": {"similarity": simi}})
        if i % 10000 == 0: print(i)
    plot.plot_CDF_Scatter([similarities], savefig='fig/similarities.png')



if __name__ == '__main__':
    crawl_pages_wrap(NUM_THREADS=1)