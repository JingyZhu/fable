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
    """
    Use chrome to load the page. Directly return the HTML text
    ID: If multi-threaded, should give ID for each thread to differentiate temp file
    """
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


def wayback_index(url, param_dict={}, wait=True):
    """
    Get the wayback machine index of certain url by querying the CDX

    return: ( [(timestamp, url)], SUCCESS/EMPTY/ERROR_MSG)
    """
    wayback_home = 'http://web.archive.org/web/'
    params = {
        'output': 'json',
        'url': url,
        'from': 19700101,
        'to': 20191231,
    }
    params.update(param_dict)
    while True:
        try:
            r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
            r = r.json()
            break
        except Exception as e:
            print(str(e))
            if not wait:
                break
            time.sleep(10)
    r = [(i[1], i[2]) for i in r[1:]]
    if len(r) != 0:
        return r, "Success",
    else:
        return [], "Empty"


def wayback_year_links(prefix, years, NUM_THREADS=10, max_limit=0, param_dict={}):
    """
    Get the result of links in certain years
    prefix: some string of url e.g: *.a.b.com/*
    years: list of years which would be query
    max_limit: Maximum #records in one retrieval
    params: Any customized params, except time range

    Should be add in try catch. In case of connection error
    """
    total_r = {}
    cur_limit = 100000 if max_limit == 0 else max_limit
    wayback_home = 'http://web.archive.org/web/'
    params = {
        'output': 'json',
        'url': prefix,
        "limit": str(cur_limit),
        'collapse': 'urlkey',
        'filter': ['statuscode:200', 'mimetype:text/html'],
    }
    params.update(param_dict)
    l = threading.Lock()
    def get_year_links(year):
        nonlocal total_r, cur_limit, max_limit
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
                assert(len(r) < cur_limit or cur_limit >= max_limit)
                break
            except Exception as e:
                print('2', str(e))
                cur_limit *= 2
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


def requests_crawl(url, timeout=20, sleep=True, html=True):
    """
    Use requests to get the page
    Return None if fails to get the content
    html: Only return html if set to true
    sleep: Will sleep if get block
    """
    while True:
        try:
            begin = time.time()
            r = requests.get(url, timeout=timeout)
            end = time.time()
            break
        except:
            if end-begin >= timeout: #timeout
                break
            time.sleep(10)
    if r.status_code >= 400:
        return
    headers = {k.lower(): v for k, v in r.headers.items()}
    content_type = headers['content-type']
    if html and 'html' not in content_type:
        return
    return r.text