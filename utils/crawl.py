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
    wayback_home = 'http://web.archive.org/web/'
    params = {
        'output': 'json',
        'url': prefix,
        "limit": '100000',
        'collapse': 'urlkey',
        'filter': ['statuscode:200', 'mimetype:text/html'],
    }
    l = threading.Lock()
    def get_half_links(year, half):
        nonlocal total_r
        total_r.setdefault(year, set())
        params.update({
            "from": "{}{}01".format(year, str(half*6).zfill(2)),
            "to": "{}{}31".format(year, str(half*6+6).zfill(2))
            # 'collapse': 'timestamp:4',
        })
        
        r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
        r = r.json()
        
        r = [u[2] for u in r[1:]]
        assert(len(r) < 100000 )
        l.acquire()
        for url in r:
            total_r[year].add(url)
        l.release()
    t = []
    year_half_orig = list(itertools.product(years, [0, 1]))
    year_half = []
    for begin in range(0, len(year_half_orig), NUM_THREADS):
        end = begin + NUM_THREADS  if begin + NUM_THREADS < len(year_half_orig) else len(year_half_orig) 
        year_half.append(year_half_orig[begin:end])
    for shot in year_half:
        print(shot)
        t = []
        for year, month in shot:
            t.append(threading.Thread(target=get_half_links, args=(year, month)))
            t[-1].start()
        for ti in t:
            ti.join()

    return {k: list(v) for k, v in total_r.items()}


    