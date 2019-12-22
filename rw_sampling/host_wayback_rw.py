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

sys.path.append('../')
from utils import crawl, url_utils
import random

NUM_HOST = 100
NUM_THREAD = 10
JUMP_RATIO = 0.1

NUM_SEEDS = 5
CHECKPOINT_INT = 10
year = 1999

counter = 0
host_extractor = url_utils.HostExtractor()

rw_stats = [] # Depth and new host_exploration stats for each walk.


def base_host(url):
    # TODO Could be modified due to real web random walking
    return host_extractor.extract(urlparse(url).netloc, wayback=True)


def keep_sampling(pools, year, wayback=True):
    """
    Keep uniform sampling until getting a url has copy in wayback machine
    Return None on all url in pools
    Wayback: If urls in pool are from wayback machine
    """
    while True:
        url = random.sample(pools, 1)[0]
        ts = str(year) + '1231235959'
        if wayback:
            last_https = url.rfind('https://')
            last_http = url.rfind('http://')
            idx = max(last_http, last_https)
            url = url[idx:]
            ts = url[idx-15:idx-1] # Extract the ts for url
        indexed_urls, _ = crawl.wayback_index(url,\
                    param_dict={'from': str(year) + '0101', 'to': str(year) + '1231'})
        if len(indexed_urls) == 0:
            continue
        indexed_urls = {int(u[0]): u[1] for u in indexed_urls}
        if wayback:
            key = min(indexed_urls.keys(), key=lambda x: abs(x-int(ts)))
        else:
            key = sorted(indexed_urls.keys())[int(len(indexed_urls)/2)]
        return indexed_urls[key]
    return None


def crawl_link(url, d):
    """
    Use chrome to load a page. 
    Extract the links and get all the html with urls 
    have difference hostname with original
    """
    outlinks = []
    home = urlparse(url).scheme + '://' + urlparse(url).netloc
    html = crawl.requests_crawl(url)
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except:
        return []
    for a_tag in soup.find_all('a'):
        if 'href' not in a_tag.attrs:
            continue
        link = a_tag.attrs['href']
        if urlparse(link).netloc == '': #Relative urls
            link = home + link
        outlinks.append(link)
    return outlinks


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
    proc_d, q_backup = mp.Manager().dict(), mp.Manager().dict()
    q_in = queue.Queue(maxsize=NUM_SEEDS+2*NUM_THREAD)
    if os.path.exists('hosts.json'):
        urls = json.load(open('hosts.json', 'r'))
        for url, v in urls.items():
            proc_d[url] = v
    if os.path.exists('Q.json'):
        url_in_Q = json.load(open('Q.json', 'r'))
        for url, v in url_in_Q.items():
            if not v:
                q_in.put(url)
                q_backup[url] = v
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
        d[hostname] = ''
        checkpoint(d, q_backup)
        outlinks = crawl_link(url, d)
        for outlink in outlinks:
            hostname = base_host(outlinks)
            collected_hosts.add(hostname)
            if hostname not in d:
                d[hostname] = ''
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
    proc_d, q_backup = {}, {}
    q_in = queue.Queue()
    proc_d, q_in, q_backup = load_checkpoint()        
    r_jump = json.load(open('url_db_2017.json', 'r'))
    r_jump = [obj['url'] for obj in r_jump]
    
    if q_in.empty(): # If there is no checkpoint before
        seeds = random.sample(r_jump, NUM_SEEDS)
        for seed in seeds:
            q_in.put((seed, 0, set()))

    pools = []
    for _ in range(NUM_THREAD):
        pools.append(threading.Thread(target=thread_func, args=(q_in, proc_d, r_jump, q_backup, year)))
        pools[-1].start()
    for t in pools:
        t.join()

    json.dump(proc_d.copy(), open('hosts.json', 'w+'))


if __name__ == '__main__':
    main()


