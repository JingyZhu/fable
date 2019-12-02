"""
Check whether wayback machine archives certain pages.
Using its API https://archive.org/help/wayback_api.php

Load Pages (most recent available pages on Wayback machine)
"""
import requests
from subprocess import *
import threading
import time
import sys
from queue import Queue
from os.path import join
import time
import json
from urllib.parse import urlparse
import platform
import os

sem = threading.Semaphore(0)
FNULL = open('/dev/null', 'w')
os_name = platform.system()

wayback_home = 'http://web.archive.org/web/'
html_wayback = {}
params = {
    'output': 'json',
    'url': None,
    'from': 19700101,
    'to': 20190927,
    'filter': 'statuscode:200'
    }

# params['url'] = 'http://www.nytimes.com/pages/opinion/index.html'
# r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
# r = r.json()
# f = open('wayback_api.json', 'w+')
# json.dump(r, f)
# exit(0)

blacklist = ['bloomberg.com', 'bloombergview.com', 'businessweek.com']
blackextension = ['.pdf']


def filter_blacklist(url_dict):
    """
    Delete the keys which are in the blacklist
    """
    new_dict = {}
    for key, value in url_dict.items():
        found = False
        for black_url in blacklist:
            if black_url in urlparse(key).netloc:
                found = True
                break
        for ext in blackextension:
            # Vprint(os.path.splitext(urlparse(key).path)[1])
            if ext == os.path.splitext(urlparse(key).path)[1]:
                found = True
                break
        if found == True:
            continue
        new_dict[key] = value
    return new_dict


def get_recent_available_links(url):
    params.update({
        'url': url,
        'limit': '-50',
        'fastLatest': 'true'
    })
    try:
        r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
        r = r.json()
    except Exception as e:
        print(str(e))
        return [], str(e)
    if len(r) != 0:
        return r, "Success"
        # return wayback_home + r[-1][1] + '/' + r[-1][2] # For last 200
    else:
        return [], "Empty"


def get_year_links(prefix, years):
    """
    Get the result of links in certain years
    prefix: some string of url
    years: list of years which would be query
    """
    params.update({
        'url': prefix,
        "from": int(str(years[0]) + '0101'),
        "to": int(str(years[-1]) + "1231"),
        "limit": '10000',
        'collapse': 'urlkey',
        # 'collapse': 'timestamp:4',
    })
    try:
        r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
        r = r.json()
    except Exception as e:
        print(str(e))
    r = r[1:]
    r = [u[2] for u in r]
    json.dump(r, open('../test/wayback_resp.json', 'w+'))


def wayback_link(links):
    """
    Get recent available (200) link from wayback from links
    """
    count = 1
    data = json.load(open('wayback_avail_count.json', 'r'))
    for link, value in links.items():
        print(count, link)
        count += 1
        if link in data:
            continue
        wayback_urls, msg = get_recent_available_links(link) 
        data[link] = { "200": len(wayback_urls) > 0, "msg": msg}
        if count % 10  == 0:
            json.dump(data, open('wayback_avail_count.json', 'w+'))

        json.dump(data, open('wayback_avail_count.json', 'w+'))


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


def main(data):
    """
    Load page in wayback_urls_sample.json
    Sample 3 timestamp and load 3 pages
    Store all the loaded HTML in wayback_html_sample.json
    """
    count = 1
    if os.path.exists('wayback_html_sample.json'):
        html_wayback = json.load(open('wayback_html_sample.json', 'r'))
    for url, value in data.items():
        print(count, url)
        count += 1
        if url in html_wayback:
            for i, ts in enumerate(html_wayback[url].keys()):
                print("Load", i, ts)
            continue
        url_list, code = get_recent_available_links(url)
        if code != "Success":
            print(url, "request to CDX server failed")
            continue
        del(url_list[0])
        size = len(url_list)
        idxs = [size-1-int(size/2), size-1-int(size/4), size-1]
        for load, idx in enumerate(idxs):
            timestamp = url_list[idx][1]
            print("Load", load, timestamp)
            wayback_url = wayback_home + url_list[idx][1] + '/' + url_list[idx][2]
            if url not in html_wayback:
                html_wayback[url] = {}
            try:
                call(['node', '../run.js', wayback_url], timeout=120)
            except Exception as e:
                print(str(e))
                call(['pkill', 'chrome'])
                continue
            html = open('temp.html', 'r').read()
            html_wayback[url][timestamp] = html

            if os.path.exists("temp.html"):
                os.remove('temp.html')
        if count % 2 == 0: # Checkpoint
            json.dump(html_wayback, open('wayback_html_sample.json', 'w+'))
    
    json.dump(html_wayback, open('wayback_html_sample.json', 'w+'))


if __name__ == '__main__':
    # data = json.load(open('wayback_urls_sample.json', 'r'))
    # # main(data)
    # # get_year_links('http://reason.com/archives/2014/04/26/living-with-inequality/', [1970, 2019])
    # url_is_leafpage = check_leafpage(data.keys())
    # json.dump(url_is_leafpage, open('../search/missing_url.json', 'w+'))
    a, _ = get_recent_available_links('https://theintercept.imgix.net/wp-uploads/sites/1/2014/10/laptop-smartphone.jpg')
    print(a)