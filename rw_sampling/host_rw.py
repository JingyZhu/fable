"""
Random Walk on host
Collect new hosts as many as possible
"""
import json
import os
import sys
import multiprocessing as mp
from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse, urljoin
import threading
import queue
import pickle

sys.path.append('../')
from utils import crawl
import random

NUM_PROC = 8
NUM_HOST = 10000
NUM_THREADs = 10
JUMP_RATIO = 0.15

counter = mp.Value('i', 0)
def base_host(url):
    hostname = urlparse(url).netloc
    if hostname.split('.')[-2] == 'co': # Assume co.** is the only exception
        hostname = hostname.split('.')[-3:]
    else:
        hostname = hostname.split('.')[-2:]
    hostname = '.'.join(hostname)
    return hostname


def request_mime(q_in, q_out, home):
    """
    Multi-threading requests url to check the mimetype
    """
    while not q_in.empty():
        url = q_in.get()
        if urlparse(url).netloc == '':
            url = urljoin(home, url)
        try:
            r = requests.get(url, timeout=10)
            if r.status_code >= 400:
                continue
            headers = {k.lower(): v for k, v in r.headers.items()}
            content_type = headers['content-type']
            if 'html' in content_type:
                q_out.put(url)
        except Exception as e:
            # print(str(e))
            continue
        

def crawl_link(url, d):
    """
    Use chrome to load a page. 
    Extract the links and get all the html with urls 
    have difference hostname with original
    """
    outlinks = []
    home = urlparse(url).scheme + '://' + urlparse(url).netloc
    html = crawl.chrome_crawl(url, timeout=60)
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except:
        return []
    q_in = queue.Queue()
    q_out = queue.Queue()
    for a_tag in soup.find_all('a'):
        if 'href' not in a_tag.attrs:
            continue
        link = a_tag.attrs['href']
        q_in.put(link)
    threads = []
    for _ in range(NUM_THREADs):
        threads.append(threading.Thread(target=request_mime, args=(q_in, q_out, home,)))
        threads[-1].start()
    for i in range(NUM_THREADs):
        threads[i].join()
    while not q_out.empty():
        outlink = q_out.get()
        hostname =  base_host(outlink)
        if hostname not in d:
            d[hostname] = ''
        outlinks.append(outlink)
    return outlinks


def checkpoint(d, Q):
    if len(d) % 100 == 0:
        json.dump(d.copy(), open('hosts.json', 'w+'))
        pickle.dump(Q, open('Q.pickle', 'wb+'))


def process_func(Q_in, d, r_jump):
    global counter
    while not Q_in.empty():
        counter.value += 1
        url = Q_in.get()
        print(counter.value, url, len(d))
        hostname = base_host(url)
        if hostname not in d:
            d[hostname] = ''
            checkpoint(d, Q_in)
        outlinks = crawl_link(url, d)
        other_host_links = [outlink for outlink in outlinks if base_host(outlink) != hostname]
        # print(other_host_links)
        if len(d) > NUM_HOST:
            continue
        elif random.random() < JUMP_RATIO or len(outlinks) < 1:
            next_url = random.sample(r_jump, 1)[0]
            Q_in.put(next_url)
        else:
            next_url = random.sample(outlinks, 1)[0] if len(other_host_links) == 0 \
                        else random.sample(other_host_links, 1)[0]
            Q_in.put(next_url)



def main():
    proc_d = mp.Manager().dict()
    Q_in = mp.Queue()
    r_jump = json.load(open('url_db_2017.json', 'r'))
    r_jump = [obj['url'] for obj in r_jump]

    # TODO 
    # Random sample sone of the urls as seeds
    seeds = random.sample(r_jump, 100)
    for seed in seeds:
        Q_in.put(seed)

    pools = []
    for _ in range(NUM_PROC):
        pools.append(mp.Process(target=process_func, args=(Q_in, proc_d, r_jump, )))
        pools[-1].start()
    for i in range(NUM_PROC):
        pools[i].join()

    json.dump(proc_d.copy(), open('hosts.json', 'w+'))


if __name__ == '__main__':
    main()


