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
import re

sys.path.append('../')
from utils import text_utils, crawl, url_utils, plot
import config

idx = config.HOSTS.index(socket.gethostname())
proxy = config.PROXIES[idx]
db = MongoClient(config.MONGO_HOSTNAME).web_decay
counter = 0

def decide_content(html):
    """
    Decide whether the page is a navigational page
    If it is, return content=empty
    Else, return the most confident content
    """
    # TODO Implement this function
    versions = ['justext', 'goose', 'newspaper']
    count = 0
    contents = []
    for v in versions:
        try:
            content = text_utils.extract_body(html, version=v)
        except Exception as e:
            print(v, str(e))
            count += 1
            if count >= 2: return ""
            continue
        if content == '': count += 1
        if count >= 2: return ""
        contents.append(content)
    if count > 0 and url_utils.find_link_density(html) >= 0.8: return ""
    return max(contents, key=lambda x: len(x.split()))


def decide_content_alt(html):
    if url_utils.find_link_density(html) >= 0.8: return ""
    return text_utils.extract_body(html, version='boilerpipe')


def get_wayback_cp(url, year):
    """
    Get multiple wayback's copies in each year if possible.
    Pick 3 of them, and pick the most likely good one

    If there is a best fit, return (ts, html, content)
    Else return None
    """
    param_dict = {
        "from": str(year) + '0101',
        "to": str(year) + '1231',
        "filter": ['statuscode:200', 'mimetype:text/html']
    }
    cps, _ = crawl.wayback_index(url, param_dict=param_dict, total_link=True, proxies=proxy)
    if len(cps) > 3: cps = random.sample(cps, 3)
    wayback = []
    for ts, cp in cps:
        html = crawl.requests_crawl(cp, proxies=proxy)
        if not html: continue
        content = decide_content_alt(html)
        if content == '': continue
        wayback.append((ts, html, content))
    if len(wayback) == 0: return
    else: return max(wayback, key=lambda x: len(x[2].split()))


def crawl_pages(q_in):
    """
    Get content from both wayback and realweb
    Update content into db.url_content
    """
    global counter
    rw_objs = []
    wm_objs = []
    while not q_in.empty():
        url, year = q_in.get()
        counter += 1
        print(counter, url)
        wayback_cp = get_wayback_cp(url, year)
        if not wayback_cp: continue # Definitely landing pages
        ts, wm_html, wm_content = wayback_cp
        rw_html = crawl.requests_crawl(url)
        if not rw_html: rw_html = ''
        rw_content = decide_content_alt(rw_html)
        rw_obj = {
            "url": url,
            "src": "realweb",
            "html": brotli.compress(rw_html.encode()),
            "content": rw_content
        }
        wm_obj = {
            "url": url,
            "src": "wayback",
            "ts": ts,
            "html": brotli.compress(wm_html.encode()),
            "content": wm_content
        }
        rw_objs.append(rw_obj)
        wm_objs.append(wm_obj)
        if len(rw_objs) >= 50 or len(wm_objs) >= 50:
            try:
                db.url_content.insert_many(rw_objs, ordered=False)
            except:
                pass
            try:
                db.url_content.insert_many(wm_objs, ordered=False)
            except:
                pass
            rw_objs, wm_objs = [], []
    try:
        db.url_content.insert_many(rw_objs, ordered=False)
    except:
        pass
    try:
        db.url_content.insert_many(wm_objs, ordered=False)
    except:
        pass


def crawl_pages_wrap(NUM_THREADS=5):
    """
    Get sampled hosts from db.host_sample
    Get all 2xx/3xx urls from sampled hosts
    """
    db.url_content.create_index([("url", pymongo.ASCENDING), ("src", pymongo.ASCENDING)], unique=True)
    db.url_content.create_index([("url", pymongo.ASCENDING)])
    q_in = queue.Queue()
    # Temporary commented
    # sampled_hosts = db.host_sample.find()
    # sampled_hosts = sorted(list(sampled_hosts), key=lambda x: x['hostname'] + str(x['year']))
    # length = len(sampled_hosts)
    # sampled_hosts = sampled_hosts[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    # urls = []
    # for sampled_host in sampled_hosts:
    #     host, year = sampled_host['hostname'], sampled_host['year']
    #     urls += list(db.url_status.find({"hostname": host, "year": year, "status": re.compile("^[23]")}))
    #End

    #Temporary added
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
            "from": "url_content",
            "localField": "_id",
            "foreignField": "url",
            "as": "contents"
        }},
        {"$match": {"contents.0": {"$exists": False}}},
    ])
    urls = list(urls)
    urls = sorted(list(urls), key=lambda x: x['_id'] + str(x['year']))
    length = len(urls)
    print(length // 4)
    urls = urls[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    # End
    
    random.shuffle(urls)
    for url in urls:
        q_in.put((url['url'], url['year']))
    pools = []
    for _ in range(NUM_THREADS):
        pools.append(threading.Thread(target=crawl_pages, args=(q_in,)))
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
    plot.plot_Scatter([similarities], savefig='fig/similarities.png')



if __name__ == '__main__':
    crawl_pages_wrap(NUM_THREADS=10)