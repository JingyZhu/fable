"""
Measure the outlink hosts differences between requests and chrome load on wayback machine
Currently only support all links. TODO: Only extract text/html as outlinks. Is that really a need?
|host(diff(chrome load, requests load))| 
[0, 0, 0, 2, 0, 0, 5, 1, 9, 0, 0, 1, 1, 0, 0, 1, 3, 0, 0, 7, 0, 0, 0, 0, 0, 0, 1, 2, 6, 9, 3, 0, 0, 10, 0, 0, -5, 0, 0, 0, -160, 0, -1, 0, 9, 2, 9, 1, 8, 0, 5, 2, 38, 4]
"""
import requests
from subprocess import call
from pymongo import MongoClient
from bs4 import BeautifulSoup
import os
import json
import sys
from urllib.parse import urlparse
import threading, queue
import time

sys.path.append('../')
from utils import crawl, url_utils
import config

db = MongoClient(config.MONGO_HOSTNAME).web_decay
host_extractor = url_utils.HostExtractor()


def get_outhosts(html):
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
        outhost_list.append(host_extractor.extract(link, wayback='web.archive.org' in link))
    return set(outhost_list)


def load_and_diff(url, ID=''):
    """
    Chrome load and request load a page
    Diff on hosts
    If bs can't break, ignore this case

    Shoud be called in a try-catch
    """
    html1 = crawl.chrome_crawl(url, timeout=240, ID=ID)
    r = requests.get(url)
    html2 = r.text
    outhosts1 = get_outhosts(html1)
    outhost2 = get_outhosts(html2)
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


def main(thread_num=8):
    data = json.load(open('sample_load_list.json', 'r'))
    data = data[:100]
    diff_dict = {}
    def thread_func(i, q_in):
        nonlocal diff_dict
        while not q_in.empty():
            url = q_in.get()
            print(i, url)
            try:
                outhost1, outhost2 = load_and_diff(url, ID=str(i))
            except Exception as e:
                print(str(e))
                continue
            diff = abs(len(outhost1) + len(outhost2) - 2* len(outhost1.intersection(outhost2)) )
            union = len(outhost1) + len(outhost2) - len(outhost1.intersection(outhost2))
            diff_dict[url] = {
                "Chrome load": len(outhost1),
                "Requests": len(outhost2),
                "Diff": diff,
                "Union": union
            }
    q_in = queue.Queue()
    for url in data:
        q_in.put(url)
    pools = []
    for i in range(thread_num):
        pools.append(threading.Thread(target=thread_func, args=(i, q_in,)))
        pools[-1].start()
    for t in pools:
        t.join()
    json.dump(diff_dict, open('links_diff.json', 'w+'))


if __name__== "__main__":
    main()