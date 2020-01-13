"""
Unlike status code checking. Page could even broken at the status code of 2xx/3xx
This scipt is used for checking 2/3xx status code
"""
import requests
import sys
from pymongo import MongoClient
import pymongo
import json
import os, threading
import brotli
import socket
import random

sys.path.append('../')
from utils import text_utils, crawl
import config

idx = config.HOSTS.index(socket.gethostname())
proxy = config.PROXIES[idx]
db = MongoClient(config.MONGO_HOSTNAME).web_decay


def get_wayback_cp(url, year):
    """
    Get multiple wayback's copies in each year if possible.
    Pick 3 of them, and pick the most likely good one

    If there is a best fit, return (ts, html, content)
    Else return None
    """
    param_dict = {
        "from": str(year) + '0101',
        "to": str(year) + '1231'
    }
    cps, _ = crawl.wayback_index(url, param_dict=param_dict, total_link=True, proxies=proxy)
    if len(cps) > 3: cps = random.sample(cps, 3)
    wayback = []
    for ts, cp in cps:
        html = crawl.requests_crawl(cp, proxies=proxy)
        if not html: continue
        try:
            content = text_utils.extract_body(html)
        except Exception as e:
            print(str(e))
            continue
        if content == '': continue
        wayback.append((ts, html, content))
    if len(wayback): return
    else: return max(wayback, key=lambda x: len(x[2].split(' ')))


def get_content(url, year):
    """
    Get content from both wayback and realweb
    Update content into db.url_content
    """
    wayback_cps = get_wayback_cp(url, year)
    if not wayback_cps: return # Definitely landing pages
    real_html = crawl.requests_crawl(url)
    if not real_html: real_html = ''


     