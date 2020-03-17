"""
Inspect temporal status of pages.
"""
import sys
from pymongo import MongoClient
import pymongo
import json, yaml
import socket
import re
from urllib.parse import urlparse
from collections import defaultdict, Counter
import requests
from dateutil import parser as dparser
import datetime
import os

sys.path.append('../')
import config
from utils import url_utils, crawl

db = MongoClient(config.MONGO_HOSTNAME).web_decay
db_test = MongoClient(config.MONGO_HOSTNAME).wd_test

PS = crawl.ProxySelector(config.PROXIES)

def broken_ts_detect(snapshots):
    """
    Detect the timestamp from when the page is broken
    Return the consequent good page vs. broken page
    """
    snapshots = sorted(snapshots, key=lambda x: int(x[0]))
    broken_idx = len(snapshots)
    for snapshot in reversed(snapshots):
        if snapshot[2][0] == '2': break
        broken_idx -= 1
    if broken_idx == len(snapshots) or broken_idx == 0: return None, None
    return snapshots[broken_idx-1], snapshots[broken_idx]


def get_techs(url_obj):
    """
    update find_tech, broken_tech and today_tech into db
    """
    url, searched_url = url_obj['url'], url_obj['searched_url']
    snapshots, _ = crawl.wayback_index(url, proxies=PS.select(), total_link=True)
    good_snap, broken_snap = broken_ts_detect(snapshots)
    if not good_snap or not broken_snap:
        print("Not broken border detected")
        db_test.reorg_tech.update_one({'_id': url}, {"$set": {"through": False}})
        return
    fine_tech = crawl.wappalyzer_analyze(good_snap[1])
    broken_ts = broken_snap[0]
    broken_deadline = dparser.parse(broken_ts) + datetime.timedelta(days=7)
    broken_deadline = broken_deadline.strftime("%Y%m%d")
    up = urlparse(url)
    host, path = up.netloc, up.path
    dirname = os.path.dirname(path)
    while True:
        wayback_urls, _ = crawl.wayback_index(host + dirname + '/*', param_dict={
            'from': broken_ts,
            'to': broken_deadline,
            'filter': ['statuscode:200', 'mimetype:text/html']
        }, total_link=True, proxies=PS.select())
        if len(wayback_urls) >= 0 or dirname in ['', '/']: break
        dirname = os.path.dirname(dirname)
    if len(wayback_urls) <=0 and broken_snap[2][0] not in ['4', '5']:
        working_url = broken_snap[1]
        working_ts = broken_snap[0]
    elif len(wayback_urls) > 0:
        working_url = wayback_urls[0][1]
        working_ts = wayback_urls[0][0]
    else:
        print("All recent same subhost urls 45xx")
        db_test.reorg_tech.update_one({'_id': url}, {"$set": {"through": False}})
        return
    broken_tech = crawl.wappalyzer_analyze(working_url)
    today_tech = crawl.wappalyzer_analyze(searched_url)
    db_test.reorg_tech.update_one({'_id': url}, {"$set": {"through": True}})
    db_test.reorg_tech.update_one({'_id': url}, {"$set": {"fine_tech": fine_tech, "broken_tech": broken_tech, "today_tech": today_tech, "fine_ts": good_snap[0], 'broken_ts': broken_snap[0], 'replace_broken_ts': working_ts}})


def inspect_reorg_tech():
    """
    Grab data of technologies that url being reorganized under different time (fine, )
    To see whether reorganization of data is because of tech changes
    """
    urls = db_test.reorg_tech.find({'broken_tech': {}})
    for i, url in enumerate(list(urls)):
        print(i, url['_id'])
        get_techs(url)


inspect_reorg_tech()