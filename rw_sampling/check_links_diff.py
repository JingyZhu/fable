"""
Measure the outlink hosts differences between requests and chrome load on wayback machine
Currently only support all links. TODO: Only extract text/html as outlinks. Is that really a need?
|host(diff(chrome load, requests load))| 
[0, 0, 0, 2, 0, 0, 5, 1, 9, 0, 0, 1, 1, 0, 0, 1, 3, 0, 0, 7, 0, 0, 0, 0, 0, 0, 1, 2, 6, 9, 3, 0, 0, 10, 0, 0, -5, 0, 0, 0, -160, 0, -1, 0, 9, 2, 9, 1, 8, 0, 5, 2, 38, 4]
"""
import requests
from subprocess import call
from pymongo import MongoClient
import pymongo
from bs4 import BeautifulSoup
import os
import json
import sys
from urllib.parse import urlparse
import threading, queue
import time

sys.path.append('../')
from utils import crawl, url_utils, plot
import config

db = MongoClient(config.MONGO_HOSTNAME).web_decay
host_extractor = url_utils.HostExtractor()


def get_outhosts(html, hostname):
    """
    Input: html text
    Output: All the outlinks' public hosts
    """
    soup = BeautifulSoup(html, 'html.parser')
    outhost_list = []
    for atag in soup.findAll('a'):
        try:
            link = atag['href']
        except:
            continue
        if urlparse(link).netloc == '': # Relative links
            outhost_list.append(hostname)
        else:
            outhost_list.append(host_extractor.extract(link, wayback='/web.archive.org/' in link))
    return set(outhost_list)


def load_and_diff(url, ID=''):
    """
    Chrome load and request load a page
    Diff on hosts
    If bs can't break, ignore this case

    Shoud be called in a try-catch
    """
    html1 = crawl.chrome_crawl(url, timeout=240, ID=ID)
    hostname = host_extractor.extract(url, wayback=True)
    while True:
        try:
            r = requests.get(url)
            break
        except Exception as e:
            print(str(e))
            time.sleep(20)
    html2 = r.text
    outhosts1 = get_outhosts(html1, hostname)
    outhost2 = get_outhosts(html2, hostname)
    return outhosts1, outhost2


def construct_wayback_url():
    """
    Construct wayback machine url for loading by querying the CDX server
    """
    data = json.load(open('status_200.json', 'r'))
    params = {'limit': 3}
    url_list = []
    for i, obj in enumerate(data):
        url, year = obj['url'], obj['year']
        params.update({
            'from': str(year) + '0101',
            'to': str(year) + '1231',
            'filter': ['statuscode:200']
        })
        while True:
            rval, code = crawl.wayback_index(url, param_dict=params)
            if code != 'Success' and code != 'Empty':
                time.sleep(20)
                continue
            if len(rval) > 0:
                url = 'http://web.archive.org/web/' + rval[0][0] + '/' + rval[0][1]
                url_list.append(url)
            break
        print(i, url, len(url_list))
    json.dump(url_list, open('sample_load_list.json', 'w+'))


def check_diff(thread_num=8):
    data = json.load(open('sample_load_list.json', 'r'))
    db.chrome_request_diff.create_index([('url', pymongo.ASCENDING)], unique=True)
    def thread_func(i, q_in):
        while not q_in.empty():
            url = q_in.get()
            print(i, url)
            try:
                outhost1, outhost2 = load_and_diff(url, ID=str(i))
            except Exception as e:
                print(str(e))
                continue
            db.chrome_request_diff.insert_one({
                'url': url,
                "chrome": list(outhost1),
                "requests": list(outhost2),
                "c-r": list(outhost1 - outhost2),
                "r-c": list(outhost2 - outhost1),
                "union": list(outhost1.union(outhost2))
            })
    q_in = queue.Queue()
    for url in data:
        q_in.put(url)
    pools = []
    for i in range(thread_num):
        pools.append(threading.Thread(target=thread_func, args=(i, q_in,)))
        pools[-1].start()
    for t in pools:
        t.join()


if __name__== "__main__":
    check_diff()