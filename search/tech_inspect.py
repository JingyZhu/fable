"""
Use wappalyzer to inspect the technologies used by websites
"""
import sys
import requests
import os
import json
from pymongo import MongoClient
import random
import multiprocessing as mp
import pymongo
import collections

sys.path.append('../')
import config
from utils import crawl, url_utils

db = MongoClient(config.MONGO_HOSTNAME).web_decay
PS = crawl.ProxySelector(config.PROXIES)

def crawl_analyze_sanity():
    """
    Crawl wayback and realweb of db.wappalyzer_sanity, and update dict into collection
    """
    urls = db.wappalyzer_sanity.find({"tech": {"$exists": False}})
    urls = list(urls)
    print("total:", len(urls))
    for i, obj in enumerate(urls):
        url = obj['_id']
        print(i, url)
        try:
            if 'web.archive.org' in url: tech = crawl.wappalyzer_analyze(url, proxy=PS.select_url())
            else: tech = crawl.wappalyzer_analyze(url, proxy=PS.select_url())
        except:
            continue
        db.wappalyzer_sanity.update_one({"_id": url}, {"$set": {"tech": tech}})


def crawl_analyze_reorg():
    """
    Crawl wayback and realweb (copies) of db.wappalyzer_reorg, and update dict into collection
    """
    urls = db.wappalyzer_reorg.find({"tech": {"$exists": False}})
    urls = list(urls)
    print("total:", len(urls))
    for i, obj in enumerate(urls):
        url = obj['_id']
        print(i, url)
        try:
            if 'web.archive.org' in url: tech = crawl.wappalyzer_analyze(url, proxy=PS.select_url())
            else: tech = crawl.wappalyzer_analyze(url, proxy=PS.select_url())
        except:
            continue
        db.wappalyzer_reorg.update_one({"_id": url}, {"$set": {"tech": tech}})

crawl_analyze_reorg()

