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
import config

NUM_HOST = 100000
NUM_THREAD = 3
JUMP_RATIO = 0.1

NUM_SEEDS = 1000
CHECKPOINT_INT = 100

year = 2009
counter = 0
host_extractor = url_utils.HostExtractor()
rw_stats = [] # Depth and new host_exploration stats for each walk.

db = MongoClient(config.MONGO_HOSTNAME).web_decay
proxies = config.PROXIES[3] # Get its proxy ip
rj_blocked = set() # Random jump blocked for faster sampling the seeds

def base_host(url):
    # TODO Could be modified due to real web random walking
    return host_extractor.extract(url, wayback=True)


def wayback_join(url, link):
    link = urljoin(url, link)
    link = link.replace('http:/', 'http://')
    link = link.replace('http:///', 'http://')
    link = link.replace('https:/', 'https://')
    link = link.replace('https:///', 'https://')
    return link


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
            url = url.replace('http://web.archive.org/web/', '')
            url = url.replace('https://web.archive.org/web/', '')
            slash = url.find('/')
            ts = ts[: slash] # Extract the ts for url
            url = url[slash + 1:]
        indexed_urls, _ = crawl.wayback_index(url,\
                    param_dict={'from': str(year) + '0101', 'to': str(year) + '1231', 
                    'filter': ['!statuscode:400', 'mimetype:text/html']}, total_link=True, proxies=proxies)
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
    html = crawl.requests_crawl(url, proxies=proxies)
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except:
        return []
    for a_tag in soup.find_all('a'):
        if 'href' not in a_tag.attrs:
            continue
        link = a_tag.attrs['href']
        if len(link) == 0 or link[0] == '#': #Anchor ignore
            continue
        link = wayback_join(url, link)
        outlinks.add(link)
    return list(outlinks)


def checkpoint(d, d_q, year):
    global counter
    if counter % CHECKPOINT_INT == 0:
        json.dump(d, open('checkpoints/hosts_{}.json'.format(year), 'w+'))
        json.dump(d_q, open('checkpoints/Q_{}.json'.format(year), 'w+'))
        # json.dump(rw_stats, open('rw_stats.json', 'w+'))


def load_checkpoint(year):
    """
    Load the checkpoint (hosts.json and Q.json) if there exists
    else, just return the init value
    """
    # global rw_stats
    proc_d, q_backup = {}, {}
    q_in = queue.Queue(maxsize=NUM_SEEDS+2*NUM_THREAD)
    if os.path.exists('checkpoints/hosts_{}.json'.format(year)):
        urls = json.load(open('checkpoints/hosts_{}.json'.format(year), 'r'))
        for url, v in urls.items():
            proc_d[url] = v
    if os.path.exists('checkpoints/Q_{}.json'.format(year)):
        url_in_Q = json.load(open('checkpoints/Q_{}.json'.format(year).format(year), 'r'))
        for url, v in url_in_Q.items():
            if v:
                q_in.put( (url,v[0], set(v[1])) )
                q_backup[url] = v
    # if os.path.exists('rw_stats.json'):
    #     rw_stats = json.load(open('rw_stats.json', 'r'))
    return proc_d, q_in, q_backup


def thread_func(q_in, d, r_jump, q_backup, year):
    """
    q_in: Input Queue for threads to consume
            (url, depth, collected_hosts)
    d: Collected host dict {hostname: value} 
    """
    global counter
    while not q_in.empty():
        counter += 1
        url, depth, collected_hosts = q_in.get()
        q_backup[url] = False
        print(counter, url, len(d))
        hostname = base_host(url)
        d[hostname] = year
        checkpoint(d, q_backup, year)
        outlinks = crawl_link(url, d)
        for outlink in outlinks:
            hostname = base_host(outlink)
            collected_hosts.add(hostname)
            if hostname not in d:
                d[hostname] = year
        other_host_links = [outlink for outlink in outlinks if base_host(outlink) != hostname]
        # print(other_host_links)
        if len(d) > NUM_HOST:
            # rw_stats.append((depth, len(collected_hosts)))
            continue
        elif random.random() < JUMP_RATIO or len(outlinks) < 1: #Random Jump
            next_url = keep_sampling(r_jump, year=year, wayback=False)
            # rw_stats.append((depth, len(collected_hosts)))
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
                # rw_stats.append((depth, len(collected_hosts)))
                q_in.put((next_url, 0, set()))
                q_backup[next_url] = (0, [])
            else:
                q_in.put((next_url, depth+1, collected_hosts))
                q_backup[next_url] = (depth+1, list(collected_hosts))


def main():
    db.hosts_meta.create_index([('hostname', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
    proc_d, q_backup = {}, {}
    q_in = queue.Queue()
    proc_d, q_in, q_backup = load_checkpoint(year)        
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

    # json.dump(rw_stats, open('rw_stats.json', 'w+'))
    objs = [{
        "hostname": hostname,
        "year": year
    } for hostname, year in proc_d.items()]
    db.hosts_meta.insert_many(objs, ordered=False)


if __name__ == '__main__':
    main()


