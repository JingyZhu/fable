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

counter = 0
host_extractor = url_utils.HostExtractor()


def base_host(url):
    # TODO Could be modified due to real web random walking
    return host_extractor.extract(urlparse(url).netloc, wayback=True)
        

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
    for outlink in outlinks:

    while not q_out.empty():
        outlink = q_out.get()
        hostname =  base_host(outlink)
        if hostname not in d:
            d[hostname] = ''
        outlinks.append(outlink)
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


def thread_func(q_in, d, r_jump, q_backup):
    """
    q_in: Input Queue for threads to consume
            (url, depth, collected_hosts)
    d: Collected host dict {hostname: value} 
    """
    global counter
    while not q_in.empty():
        counter += 1
        url, depth, collected_hosts = q_in.get()
        q_backup[url] = 1
        print(counter, url, len(d))
        hostname = base_host(url)
        d[hostname] = ''
        checkpoint(d, q_backup)
        outlinks = crawl_link(url, d)
        other_host_links = [outlink for outlink in outlinks if base_host(outlink) != hostname]
        # print(other_host_links)
        if len(d) > NUM_HOST:
            continue
        elif random.random() < JUMP_RATIO or len(outlinks) < 1:
            next_url = random.sample(r_jump, 1)[0]
            q_in.put(next_url)
            q_backup[next_url] = 0
        else:
            next_url = random.sample(outlinks, 1)[0] if len(other_host_links) == 0 \
                        else random.sample(other_host_links, 1)[0]
            q_in.put(next_url)
            q_backup[next_url] = 0


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
        pools.append(threading.Thread(target=thread_func, args=(q_in, proc_d, r_jump, q_backup)))
        pools[-1].start()
    for t in pools:
        t.join()

    json.dump(proc_d.copy(), open('hosts.json', 'w+'))


if __name__ == '__main__':
    main()


