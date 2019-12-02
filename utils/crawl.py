"""
Utilities for crawling a page
"""
from subprocess import call
import requests 
import os
import time
from os.path import abspath, dirname, join

def chrome_crawl(url, timeout=120):
    try:
        cur = str(int(time.time())) + '_' + str(os.getpid()) 
        file = cur + '.html'
        call(['node', join(dirname(abspath(__file__)), 'run.js'), url, cur], timeout=timeout)
    except Exception as e:
        print(str(e))
        pid = open(file, 'r').read()
        call(['kill', '-9', pid])
        os.remove(file)
        return ""
    html = open(file, 'r').read()
    os.remove(file)
    return html