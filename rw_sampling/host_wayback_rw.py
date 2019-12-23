"""
Random Walk on host
Collect new hosts as many as possible
"""
import json
import os
import sys
from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse, urljoin
import threading
import queue
import pickle
from pymongo import MongoClient
import pymongo

sys.path.append('../')
from utils import crawl, url_utils
import random

NUM_HOST = 1000
NUM_THREAD = 1
JUMP_RATIO = 0.1

NUM_SEEDS = 10
CHECKPOINT_INT = 10

year = 1999
counter = 0
host_extractor = url_utils.HostExtractor()
rw_stats = [] # Depth and new host_exploration stats for each walk.

db = MongoClient().web_decay
rj_blocked = set() # Random jump blocked for faster sampling the seeds

def base_host(url):
    # TODO Could be modified due to real web random walking
    return host_extractor.extract(urlparse(url).netloc, wayback=True)


def keep_sampling(pools, year, wayback=True):
    """
    Keep uniform sampling until getting a url has copy in wayback machine
    Return None on all url in pools
    Wayback: If urls in pool are from wayback machine
    """
    blocked = set() if wayback else rj_blocked
    while len(blocked) < len(pools):
        idx = random.randint(0, len(pools)-1)
        while idx in blocked:
            idx = random.randint(0, len(pools)-1)
        url = pools[idx]
        ts = str(year) + '1231235959'
        if wayback:
            last_https = url.rfind('https://')
            last_http = url.rfind('http://')
            idx = max(last_http, last_https)
            ts = url[idx-15:idx-1] # Extract the ts for url
            url = url[idx:]
        # print(url)
        indexed_urls, _ = crawl.wayback_index(url,\
                    param_dict={'from': str(year) + '0101', 'to': str(year) + '1231', 
                    'filter': ['!statuscode:400']}, total_link=True)
        if len(indexed_urls) == 0:
            blocked.add(idx)
            continue
        indexed_urls = {int(u[0]): u[1] for u in indexed_urls}
        if wayback:
            key = min(indexed_urls.keys(), key=lambda x: abs(x-int(ts)))
        else:
            key = sorted(indexed_urls.keys())[int(len(indexed_urls)/2)]
        print("Sampled:", indexed_urls[key])
        return indexed_urls[key]
    print("Sampled None")
    return None


def crawl_link(url, d):
    """
    Use chrome to load a page. 
    Extract the links and get all the html with urls 
    have difference hostname with original
    """
    outlinks = set()
    # home = urlparse(url).scheme + '://' + urlparse(url).netloc + '/'
    html = crawl.requests_crawl(url)
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except:
        return []
    for a_tag in soup.find_all('a'):
        if 'href' not in a_tag.attrs:
            continue
        link = a_tag.attrs['href']
        if link[0] == '#': #Anchor ignore
            continue
        link = urljoin(url, link)
        outlinks.add(link)
    return list(outlinks)


def checkpoint(d, d_q):
    global counter
    if counter % CHECKPOINT_INT == 0:
        json.dump(d, open('hosts.json', 'w+'))
        json.dump(d_q, open('Q.json', 'w+'))


def load_checkpoint():
    """
    Load the checkpoint (hosts.json and Q.json) if there exists
    else, just return the init value
    """
    proc_d, q_backup = {}, {}
    q_in = queue.Queue(maxsize=NUM_SEEDS+2*NUM_THREAD)
    if os.path.exists('hosts.json'):
        urls = json.load(open('hosts.json', 'r'))
        for url, v in urls.items():
            proc_d[url] = v
    if os.path.exists('Q.json'):
        url_in_Q = json.load(open('Q.json', 'r'))
        for url, v in url_in_Q.items():
            if not v:
                q_in.put(tuple([url] + v))
                q_backup[url] = tuple([url] + v)
    return proc_d, q_in, q_backup


def thread_func(q_in, d, r_jump, q_backup, year):
    """
    q_in: Input Queue for threads to consume
            (url, depth, collected_hosts)
    d: Collected host dict {hostname: value} 
    """
    global counter, rw_stats
    while not q_in.empty():
        counter += 1
        url, depth, collected_hosts = q_in.get()
        q_backup[url] = 1
        print(counter, url, len(d), depth, len(collected_hosts))
        hostname = base_host(url)
        d[hostname] = year
        checkpoint(d, q_backup)
        outlinks = crawl_link(url, d)
        for outlink in outlinks:
            hostname = base_host(outlink)
            collected_hosts.add(hostname)
            if hostname not in d:
                d[hostname] = year
        other_host_links = [outlink for outlink in outlinks if base_host(outlink) != hostname]
        # print(other_host_links)
        if len(d) > NUM_HOST:
            continue
        elif random.random() < JUMP_RATIO or len(outlinks) < 1: #Random Jump
            next_url = keep_sampling(r_jump, year=year, wayback=False)
            rw_stats.append((depth, len(collected_hosts)))
            q_in.put((next_url, 0, set()))
            q_backup[next_url] = (0, [])
        else:
            if len(other_host_links) > 0:
                next_url = keep_sampling(other_host_links, year=year)
                next_url = next_url if next_url is not None else keep_sampling(outlinks, year=year)
            else:
                next_url = keep_sampling(outlinks, year=year)
            if next_url is None:
                next_url = keep_sampling(r_jump, year=year, wayback=False)
                rw_stats.append((depth, len(collected_hosts)))
                q_in.put((next_url, 0, set()))
                q_backup[next_url] = (0, [])
            else:
                q_in.put((next_url, depth+1, collected_hosts))
                q_backup[next_url] = (depth+1, list(collected_hosts))


def main():
    db.hosts_meta.create_index([('url', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
    proc_d, q_backup = {}, {}
    q_in = queue.Queue()
    proc_d, q_in, q_backup = load_checkpoint()        
    r_jump = list(db.seeds.find({}, {"_id": False, "url": True}))
    r_jump = [u['url'] for u in r_jump]
    
    if q_in.empty(): # If there is no checkpoint before
        seeds = []
        for _ in range(NUM_SEEDS):
            seeds.append(keep_sampling(r_jump, year=year, wayback=False))
        for seed in seeds:
            q_in.put((seed, 0, set()))

    pools = []
    for _ in range(NUM_THREAD):
        pools.append(threading.Thread(target=thread_func, args=(q_in, proc_d, r_jump, q_backup, year)))
        pools[-1].start()
    for t in pools:
        t.join()

    json.dump(rw_stats, open('rw_stats.json', 'w+'))
    objs = [{
        "hostname": hostname,
        "year": year
    } for hostname, year in proc_d.items()]
    db.hosts_meta.insert_many(objs, ordered=False)


if __name__ == '__main__':
    main()


