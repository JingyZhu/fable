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
import threading
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


def chrome_load(url):
    """
    Load a page using chrome
    return a url list 
    """
    html = crawl.chrome_crawl(url)
    outhosts = get_outhosts(html)
    return outhosts



def requests_load(url):
    """
    request.get a page
    """
    try:
        r = requests.get(url)
    except:
        return []
    html = r.text
    outhosts = get_outhosts(html)
    return outhosts


def main():
    data = json.load(open('status_200.json', 'r'))
    params = {'limit': 3}
    url_list = []
    for i, obj in enumerate(data):
        url, year = obj['url'], obj['year']
        print(i, url)
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
    json.dump(url_list, open('sample_load_list.json', 'w+'))


    # diff_dict = {}
    # for i, url in enumerate(data):
    #     print(i, url)
    #     outlink1 = chrome_load(url)
    #     outlink2 = requests_load(url)
    #     diff_dict[url] = len(outlink1) - len(outlink2)
    # print(list(diff_dict.values()))

if __name__== "__main__":
    main()