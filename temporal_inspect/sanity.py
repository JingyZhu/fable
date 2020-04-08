"""
Sanity check for some results
"""
import sys
from pymongo import MongoClient
import pymongo
import json, yaml
import re, random
from urllib.parse import urlparse
from collections import defaultdict, Counter
import requests
import datetime
import os
from dateutil import parser as dparser
import itertools

sys.path.append('../')
import config
from utils import url_utils, crawl

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
db_test = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').wd_test

PS = crawl.ProxySelector(config.PROXIES)

def same_tech(a, b):
    a = {k: sorted(v) for k, v in a.items()}
    b = {k: sorted(v) for k, v in b.items()}
    return a == b

def tech_change_nobroken_verify():
    """
    Entry func
    For subhosts with no broken urls after landing page changes, see whether urls really change the tech
    """
    wayback_home = 'http://web.archive.org/web/'
    count = 0
    urls = db.site_url_before_after.aggregate([
        {"$match": {"type": "Change"}},
        {"$group": {
            "_id":{
                "periodID": "$periodID",
                "subhost": "$subhost",
            }, 
            "total": {"$sum": 1},
            "urls": {"$push": {
                "url": "$url",
                "beforeTS": "$beforeTS",
                "afterTS": "$afterTS",
                "afterStatus": "$afterStatus"
            }}
        }},
        {"$match": {"total": {"$gte": 50}}}
    ], allowDiskUse=True)
    df_dict = {}
    for url in list(urls):
        url['_id'].update({"afterStatus": re.compile('^([34]|-)'), "type": "Change"})
        broken_urls_count = db.site_url_before_after.count_documents(url['_id'])
        if broken_urls_count / url['total'] > 0.05: continue
        df_dict[url['_id']['subhost'] + '_' + str(url['_id']['periodID'])] = url['urls']
    df_dict = {k: df_dict[k] for k in random.sample(list(df_dict), 100)}
    for subhost_period, urls in df_dict.items():
        print(count, subhost_period)
        count += 1
        urls = list(filter(lambda x: re.compile('^[2]').match(x['afterStatus']) is not None, urls))
        urls = random.sample(urls, 10)
        subhost, period = subhost_period.split('_')
        for url in urls:
            print(url['url'])
            before_url = wayback_home + url['beforeTS'] + '/' + url['url'] 
            after_url = wayback_home + url['afterTS'] + '/' + url['url'] 
            before_tech = crawl.wappalyzer_analyze(before_url)
            after_tech = crawl.wappalyzer_analyze(after_url)
            obj = url.copy()
            obj.update({
                'subhost': subhost,
                'periodID': period,
                'beforeTech': before_tech,
                'afterTech': after_tech
            })
            db_test.tech_change_nobroken.insert_one(obj)

tech_change_nobroken_verify()
