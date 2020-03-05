"""
Utilities for crawling a page
"""
from subprocess import call, check_output
import requests 
import os
import time
from os.path import abspath, dirname, join
import base64
import threading, queue
import itertools
import cchardet
from reppy.robots import Robots
from urllib.parse import urlparse
import json
from collections import defaultdict
from itertools import product

class ProxySelector:
    """
    Select Proxy from a pool
    """
    def __init__(self, proxies):
        self.proxies = proxies
        self.len = len(proxies)
        self.idx = 0

    def select(self):
        """ Currently RR """
        self.idx = self.idx + 1 if self.idx < self.len -1 else 0
        return self.proxies[self.idx]
    
    def select_url(self, scheme='http'):
        """ Directly return url instead of dict """
        proxy = self.select()
        if proxy == {}: return
        else: return "{}://{}".format(scheme, proxy[scheme] )


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


def wayback_index(url, param_dict={}, wait=True, total_link=False, proxies={}):
    """
    Get the wayback machine index of certain url by querying the CDX
    wait: wait unitl not getting block
    total_link: Returned url are in full links

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
    count = 0
    while True:
        try:
            r = requests.get('http://web.archive.org/cdx/search/cdx', params=params, proxies=proxies)
            r = r.json()
            break
        except Exception as e:
            try:
                print('Wayback index:', r.text.split('\n')[0])
            except Exception as e:
                count += 1
                if count < 3:
                    time.sleep(5)
                    continue 
                return [], str(e)
            if not wait or r.status_code != 429:
                return [], str(e)
            time.sleep(10)
    if total_link:
        r = [(i[1], "{}{}/{}".format(wayback_home, i[1], i[2])) for i in r[1:]]
    else:
        r = [(i[1], i[2]) for i in r[1:]]
    if len(r) != 0:
        return r, "Success",
    else:
        return [], "Empty"


def wayback_year_links(prefix, years, NUM_THREADS=3, max_limit=0, param_dict={}, proxies={}):
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
    def get_year_links(q_in):
        nonlocal total_r, cur_limit, max_limit
        while not q_in.empty():
            year = q_in.get()
            total_r.setdefault(year, set())
            params.update({
                "from": "{}0101".format(year),
                "to": "{}1231".format(year)
                # 'collapse': 'timestamp:4',
            })
            
            while True:
                try:
                    r = requests.get('http://web.archive.org/cdx/search/cdx', params=params, proxies=proxies)
                    r = r.json()
                    r = [u[2] for u in r[1:]]
                except Exception as e:
                    print('1', str(e))
                    time.sleep(10)
                    continue
                try:
                    assert(len(r) < cur_limit or cur_limit >= max_limit)
                    break
                except Exception as e:
                    print('2', str(e))
                    cur_limit *= 2
                    params.update({'limit': str(cur_limit)})
                    continue
            print( (year, len(r)) )
            l.acquire()
            for url in r:
                total_r[year].add(url)
            l.release()
    t = []
    q_in = queue.Queue(maxsize=len(years) + 1)
    for year in years:
        q_in.put(year)
    for _ in range(NUM_THREADS):
        t.append(threading.Thread(target=get_year_links, args=(q_in,)))
        t[-1].start()
    for tt in t:
        tt.join() 

    return {k: list(v) for k, v in total_r.items()}


def requests_crawl(url, timeout=20, wait=True, html=True, proxies={}):
    """
    Use requests to get the page
    Return None if fails to get the content
    html: Only return html if set to true
    wait: Will wait if get block
    """
    filter_ext = ['.pdf']
    requests_header = {'user-agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"}
    if os.path.splitext(url)[1] in filter_ext: return
    count = 0
    while True:
        try:
            r = requests.get(url, timeout=timeout, proxies=proxies, headers=requests_header)
            if wait and (r.status_code == 429 or r.status_code == 504) and count < 3:  # Requests limit
                print('requests_crawl:', 'get status code', str(r.status_code))
                count += 1
                time.sleep(10)
                continue
            break
        except:
            print("There is an exception with requests_crawl")
            return
    if r.status_code >= 400:
        return
    headers = {k.lower(): v for k, v in r.headers.items()}
    content_type = headers['content-type'] if 'content-type' in headers else ''
    if html and 'html' not in content_type:
        return
    r.encoding = r.apparent_encoding
    text = r.text
    return text


def get_sitemaps(hostname):
    """
    Trying to find the sitemap of a site
    TODO Iterate over sitemap trees to find all the urls
    """
    requests_header = {'user-agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"}
    try:
        r = requests.get('http://{}/'.format(hostname), headers=requests_header, timeout=10)
    except: return None
    hostname = urlparse(r.url).netloc
    robots_url = 'http://{}/robots.txt'.format(hostname)
    try:
        rp = Robots.fetch(robots_url, headers=requests_header, timeout=10)
        sitemaps = rp.sitemaps
    except: sitemaps = []
    if len(sitemaps) > 0: return sitemaps
    sitemap_url = 'http://{}/sitemap.xml'.format(hostname)
    try:
        r = requests.get(sitemap_url, headers=requests_header, timeout=10)
        if r.status_code >= 400: return None
        else: return [sitemap_url]
    except: return None


def wappalyzer_analyze(url, proxy=None):
    """
    Use wappalyzer to analyze the tech used by this website
    """
    agent_string = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"
    focus_categories = {
        "1": "CMS", 
        "6": "Ecommerce",
        "18": "Web frameworks",
        "19": "Miscellaneous",
        "22": "Web servers", 
        "27": "Programming Languages", 
        "28": "Operating Systems",
        "34": "Databases", 
        "64": "Reverse proxies"
    }
    tech = defaultdict(list)
    browsers = ['zombie', 'puppeteer']
    for browser in browsers:
        try:
            if proxy: cmd = ['wappalyzer', '-b', browser, '-a', agent_string,  '--proxy', proxy, url]
            else: cmd = cmd = ['wappalyzer', '-b', browser, '-a', agent_string, url]
            output = check_output(cmd, timeout=15)
            result = json.loads(output.decode())
        except Exception as e:
            print('Wappalyzer in crawl:', str(e))
            continue
        if len(result['applications']) > 0: break
        print('continue', browser, url)
    for obj in result['applications']:
        for cate in obj['categories']:
            key = list(cate.keys())[0]
            if key in focus_categories:
                tech[focus_categories[key]].append(obj['name'])
    return tech