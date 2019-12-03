"""
Utilities for crawling a page
"""
from subprocess import call
import requests 
import os
import time
from os.path import abspath, dirname, join
import base64

def chrome_crawl(url, timeout=120, screenshot=False):
    try:
        cur = str(int(time.time())) + '_' + str(os.getpid()) 
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
        return "", ""
    html = open(file, 'r').read()
    os.remove(file)
    img = open(cur + '.jpg', 'r').read()
    os.remove(cur + '.jpg')
    if screenshot:
        url_file = url.replace('http:', '')
        url_file = url_file.replace('https:', '')
        url_file = url_file.replace('/', '-')
        f = open(url_file + '.jpg', 'wb+')
        f.write(base64.b64decode(img))
        f.close()
    return html, url_file + 'jpg'