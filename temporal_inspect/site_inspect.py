"""
Inspect temporal status (change) of sites.
"""
import sys
from pymongo import MongoClient
import pymongo
import json, yaml
import re
from urllib.parse import urlparse
from collections import defaultdict, Counter
import requests
import datetime
import os

sys.path.append('../')
import config
from utils import url_utils, crawl

db = MongoClient(config.MONGO_HOSTNAME).web_decay
db_test = MongoClient(config.MONGO_HOSTNAME).wd_test

PS = crawl.ProxySelector(config.PROXIES)


def subhost_tech_change(subhost):
    homepage_snapshots, _ = crawl.wayback_index(subhost, total_link=True, param_dict={"filter": "statuscode:200"})
    homepage_snapshots = sorted(homepage_snapshots, key=lambda x: int(x[0]))
    begin_idx, end_idx = 0, len(homepage_snapshots)-1
    begin_ts, end_ts = homepage_snapshots[0][0], homepage_snapshots[-1][0]
    def check_transfer(begin_idx, end_idx, records):
        transfer = []
        if begin_idx == end_idx:
            return transfer
        begin_tech = crawl.wappalyzer_analyze(records[begin_idx][1])
        end_tech = crawl.wappalyzer_analyze(records[end_idx][1])
        if begin_tech == end_tech: 
            return transfer
        elif end_idx - begin_idx <= 1:
            return [(records[begin_idx][0], begin_tech), (records[end_idx][0], end_tech)]
        else:
            transfer = check_transfer(begin_idx, (begin_idx + end_idx) // 2, records) \
                       + transfer \
                       + check_transfer((begin_idx + end_idx) // 2, end_idx, records)
            return transfer
    tech_transfer_records = check_transfer(begin_idx, end_idx, homepage_snapshots)
    # TODO: Finish this function to determine period of techs AND FIND WAY TO DEFINE TWO DICT AS SAME!
    # for tech_transfer in tech_transfer_records:




def collect_tech_change_sites():
    """
    Collect site's (subhosts) tech change time range 
    """
    subhosts = db.site_tech.find()
    subhosts = list(subhosts)
    for subhost in subhosts:

    