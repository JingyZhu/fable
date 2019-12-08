"""
Utilities for crawling a page
"""
from subprocess import call
import requests 
import os
import time
from os.path import abspath, dirname, join
import base64

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
    



    