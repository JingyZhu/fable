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

def decide_content(html):
    if url_utils.find_link_density(html) >= 0.8: return ""
    return text_utils.extract_body(html, version='domdistiller')

def reextract_content(NUM_THREADS=10):
    """
    Re-Extracting content from html using new method
    If wayback has no conten, unset the content field.
    """
    q_in = queue.Queue()
    counter = 0
    # Get content of url_status join host_sample, which is not in url_content 
    def thread_func(q_in):
        nonlocal counter
        while not q_in.empty():
            obj = q_in.get()
            counter += 1
            print(counter, obj['url'])
            html = brotli.decompress(obj['html']).decode()
            try:
                content = decide_content(html)
            except Exception as e:
                print('Decide content', str(e))
                content = ""
            if content == '' and obj['src'] == 'wayback':
                db.url_content.update_one({'url': obj['url'], 'src': 'wayback'}, {"$unset": {"content": ""}})
            else:
                db.url_content.update_one({'url': obj['url'], 'src': obj['src']}, {"$set": {"content": content}})
    
    urls = list(db.url_content.find())
    urls = sorted(urls, key=lambda x: x['url'])
    length = len(urls)
    print(length // 4)
    urls = urls[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    
    random.shuffle(urls)
    for obj in urls:
        q_in.put(obj)
    pools = []
    for _ in range(NUM_THREADS):
        pools.append(threading.Thread(target=thread_func, args=(q_in,)))
        pools[-1].start()
    for t in pools:
        t.join()


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
        rw_content = decide_content(rw_html)
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

if __name__ == '__main__':
    reextract_content()