"""
Utilities for crawling a page
"""
from subprocess import call
import requests 
import os
import time
from os.path import abspath, dirname, join
import base64
import threading
import itertools

def chrome_crawl(url, timeout=120, screenshot=False, ID=''):
    try:
        cur = str(int(time.time())) + '_' + str(os.getpid()) + ID
        file = cur + '.html'
        cmd = ['node', join(dirname(abspath(__file__)), 'run.js'), url, '--filename', cur]
        if screenshot:
            cmd.append('--screenshot')
        call(cmd, timeout=timeout)
    except Exception as e:
        print(str(e))
        pid = open(file, 'r').read()
        call(['kill', '-9', pid])
        os.remove(file)
        return "" if not screenshot else "", ""
    html = open(file, 'r').read()
    os.remove(file)
    if not screenshot:
        return html

    img = open(cur + '.jpg', 'r').read()
    os.remove(cur + '.jpg')
    url_file = url.replace('http://', '')
    url_file = url_file.replace('https://', '')
    url_file = url_file.replace('/', '-')
    f = open(url_file + '.jpg', 'wb+')
    f.write(base64.b64decode(img))
    f.close()
    return html, url_file + 'jpg'


def wayback_index(url, param_dict={}):
    """
    Get the wayback machine index of certain url by querying the CDX
    """
    wayback_home = 'http://web.archive.org/web/'
    params = {
        'output': 'json',
        'url': url,
        'from': 19700101,
        'to': 20191231,
    }
    params.update(param_dict)
    try:
        r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
        r = r.json()
    except Exception as e:
        print(str(e))
        return [], str(e)
    r = [(i[1], i[2]) for i in r[1:]]
    if len(r) != 0:
        return r, "Success",
    else:
        return [], "Empty"


def wayback_year_links(prefix, years, NUM_THREADS=10):
    """
    Get the result of links in certain years
    prefix: some string of url e.g: *.a.b.com/*
    years: list of years which would be query

    Should be add in try catch. In case of connection error
    """
    total_r = {}
    cur_limit = 100000
    wayback_home = 'http://web.archive.org/web/'
    params = {
        'output': 'json',
        'url': prefix,
        "limit": str(cur_limit),
        'collapse': 'urlkey',
        'filter': ['statuscode:200', 'mimetype:text/html'],
    }
    l = threading.Lock()
    def get_year_links(year):
        nonlocal total_r, cur_limit
        total_r.setdefault(year, set())
        params.update({
            "from": "{}0101".format(year),
            "to": "{}1231".format(year)
            # 'collapse': 'timestamp:4',
        })
        
        while True:
            try:
                r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
                r = r.json()
                r = [u[2] for u in r[1:]]
            except Exception as e:
                print('1', str(e))
                time.sleep(20)
                continue
            try:
                assert(len(r) < cur_limit )
                break
            except Exception as e:
                print('2', str(e))
                cur_limit += 50000
                params.update({'limit': str(cur_limit)})
                continue
        l.acquire()
        for url in r:
            total_r[year].add(url)
        l.release()
    t = []
    batch = []
    for begin in range(0, len(years), NUM_THREADS):
        end = begin + NUM_THREADS  if begin + NUM_THREADS < len(years) else len(years) 
        batch.append(years[begin:end])
    for shot in batch:
        t = []
        for year in shot:
            t.append(threading.Thread(target=get_year_links, args=(year,)))
            t[-1].start()
        for ti in t:
            ti.join()

    return {k: list(v) for k, v in total_r.items()}


    