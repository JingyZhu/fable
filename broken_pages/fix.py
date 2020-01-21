"""
Fix some error when doing the first crawl
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

sys.path.append('../')
from utils import text_utils, crawl, url_utils, plot
import config

idx = config.HOSTS.index(socket.gethostname())
proxy = config.PROXIES[idx]
db = MongoClient(config.MONGO_HOSTNAME).web_decay
counter = 0

def decide_content_alt(html):
    if url_utils.find_link_density(html) >= 0.8: return ""
    return text_utils.extract_body(html, version='boilerpipe')


def crawl_pages(q_in):
    """
    Get content from both wayback and realweb
    Update content into db.url_content
    """
    global counter
    while not q_in.empty():
        url, year = q_in.get()
        counter += 1
        print(counter, url)
        rw_html = crawl.requests_crawl(url)
        if not rw_html: rw_html = ''
        rw_content = decide_content_alt(rw_html)
        rw_obj = {
            "url": url,
            "src": "realweb",
            "html": brotli.compress(rw_html.encode()),
            "content": rw_content
        }
        db.url_content.update_one({"url": url, "src": "realweb"}, 
                {"$set": rw_obj}, upsert=True)


def fix(NUM_THREADS=10):
    """
    Re encode previously 'ISO-8859-1' to 'utf-8'
    Re requests to realweb pages
    """
    q_in = queue.Queue()
    urls = db.url_status_implicit_broken.find({"similarity": {"$exists": True, "$lt": 0.8}})
    urls = list(urls)
    urls = sorted(list(urls), key=lambda x: x['_id'] + str(x['year']))
    length = len(urls)
    print(length // 4)
    urls = urls[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    
    random.shuffle(urls)
    for url in urls:
        q_in.put((url['url'], url['year']))
    pools = []
    for _ in range(NUM_THREADS):
        pools.append(threading.Thread(target=crawl_pages, args=(q_in,)))
        pools[-1].start()
    for t in pools:
        t.join()

fix()