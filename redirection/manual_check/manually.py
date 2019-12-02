from subprocess import call
from pymongo import MongoClient
import csv
import json
import requests
import random
import os
from urllib.parse import urlparse
import threading

PORT1 = 9222
PORT2 = 9223

wayback_home = 'http://web.archive.org/web/'
html_wayback = {}
params = {
    'output': 'json',
    'url': None,
    'from': 19700101,
    'to': 20190927,
    'filter': 'statuscode:200'
    }


def check_leafpage(links):
    """
    Check whether a url is leaf page on the website by querying for prefix on wayback machine
    Return a dict with url: isLeafNode
    """
    url_leafdict = {}
    for i, link in enumerate(links):
        print(i, link)
        link_parse = urlparse(link)
        if link_parse.path == '' or link_parse.path == '/': # Homepage
            url_leafdict[link] = False
            continue
        q_link, _ = os.path.splitext(link)
        q_link += '' if link[-1] == '/' else '/'
        params.update({
            "limit": -1000,
            'collapse': 'timestamp:4',
            'url': q_link,
            'matchType': 'prefix',
            'filter': ['statuscode:200', 'mimetype:text/html'],
        })
        try:
            r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
            r = r.json()
        except Exception as e:
            print(str(e))
            continue
        if len(r) == 0:
            url_leafdict[link] = True
            continue
        del r[0]
        r = list(filter(lambda x: 'feed' not in x[0][-5:], r))
        url_leafdict[link] = not (len(r) > 0)
    return url_leafdict

def sample():
    web_decay = MongoClient().web_decay
    all_urls = []
    for obj in web_decay.html.find({}, {'url': 1, 'wayback_url': 1}):
        all_urls.append({
            "url": obj['url'],
            "wayback_url": obj['wayback_url']
        })
    links = random.sample(all_urls, 120)
    url_leafdict = check_leafpage([o['url'] for o in links])
    links = list(filter(lambda x: x['url'] in url_leafdict and url_leafdict[x['url']], links))
    json.dump(links, open('test_sample.json', 'w+'))

def thread_wrapper(args, timeout):
    call(args, timeout=timeout)

def load_sample_pages():
    """
    Load sample pages from test_sample.json
    Label the page manually
    Store the result to label.json
    """
    lookup = {
        "Same": 2,
        "Unsure": 1,
        "Miss": 0
    }
    links = json.load(open('test_sample.json'))
    label = []
    for i, link in enumerate(links):
        url, wayback_url = link['url'], link['wayback_url']
        t1 = threading.Thread(target=call, args=(['node', 'navigate.js', url, str(PORT1)], 120) )
        t1.start()
        t2 = threading.Thread(target=call, args=(['node', 'navigate.js', wayback_url, str(PORT2)], 120))
        t2.start()
        t1.join()
        t2.join()
        while True:
            try:
                result = input("Same/Miss/Unsure? ")
                link['label'] = lookup[result]
                label.append(link)
                break
            except:
                continue

    json.dump(label, open('label.json', 'w+'))


def main():
    man_label = json.load(open('label.json', 'r'))
    redirection = MongoClient().web_decay.redirection
    thres = [0.4, 0.6, 1.1]
    mat = [[0 for _ in range(3)] for _ in range(3)]
    for obj in man_label:
        url, label = obj['url'], obj['label']
        simi = redirection.find_one({'url': url})['similarity']
        cate = next(x[0] for x in enumerate(thres) if x[1] >= simi)
        mat[label][cate] += 1
    print(mat)


if __name__ == '__main__':
    main()