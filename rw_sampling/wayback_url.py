"""
Crawl the whole links on certain years
"""
import requests
import json
from urllib.parse import urlparse
import threading
from queue import Queue
import itertools

wayback_home = 'http://web.archive.org/web/'
html_wayback = {}
params = {
    'output': 'json',
    'url': None,
    'from': 19700101,
    'to': 20191114,
    'filter': 'statuscode:200'
}
        

def get_year_links(prefix, years):
    """
    Get the result of links in certain years
    prefix: some string of url
    years: list of years which would be query
    """
    total_r = {}
    l = threading.Lock()
    def get_half_links(year, half):
        nonlocal total_r
        total_r.setdefault(year, set())
        params.update({
            'url': prefix,
            "from": "{}{}01".format(year, str(half*6).zfill(2)),
            "to": "{}{}31".format(year, str(half*6+6).zfill(2)),
            "limit": '100000',
            'collapse': 'urlkey',
            'filter': ['statuscode:200', 'mimetype:text/html'],
            # 'collapse': 'timestamp:4',
        })
        try:
            r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
            r = r.json()
        except Exception as e:
            print(str(e))
        r = [u[2] for u in r[1:]]
        assert(len(r) < 100000 )
        l.acquire()
        for url in r:
            total_r[year].add(url)
        l.release()
    t = []
    year_half_orig = list(itertools.product(years, [0, 1]))
    year_half = []
    for begin in range(0, len(year_half_orig), 10):
        end = begin + 10  if begin + 10 < len(year_half_orig) else len(year_half_orig) 
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



def get_recent_available_links(url):
    params.update({
        'url': url,
        'limit': '50',
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


if __name__ == '__main__':
    rval = get_year_links("*.forbes.com/*", list(range(2000, 2020)))
    json.dump(rval, open('wayback_urls.json', 'w+'))