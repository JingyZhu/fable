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


def same_tech(a, b):
    a = {k: sorted(v) for k, v in a.items()}
    b = {k: sorted(v) for k, v in b.items()}
    return a == b


def subhost_tech_change(subhost):
    homepage_snapshots, _ = crawl.wayback_index(subhost, total_link=True, param_dict={"filter": "statuscode:200"}, proxies=PS.select())
    homepage_snapshots = sorted(homepage_snapshots, key=lambda x: int(x[0]))
    if len(homepage_snapshots) <=1:
        return []
    begin_idx, end_idx = 0, len(homepage_snapshots) - 1
    begin_ts, end_ts = homepage_snapshots[0][0], homepage_snapshots[-1][0]
    def wappalyzer_once(obj, visits):
        """Only crawl certain ts once across all search"""
        ts, url, _ = obj
        if ts in visits: return visits[ts]
        tech = crawl.wappalyzer_analyze(url)
        visits[ts] = tech
        return tech

    def check_transfer(begin_idx, end_idx, records, visits):
        transfer = []
        print('period:', begin_idx, end_idx)
        if begin_idx == end_idx:
            return transfer
        begin_tech = wappalyzer_once(records[begin_idx], visits)
        end_tech = wappalyzer_once(records[end_idx], visits)
        if same_tech(begin_tech, end_tech):
            return transfer
        elif end_idx - begin_idx <= 1:
            return [(records[begin_idx][0], begin_tech), (records[end_idx][0], end_tech)]
        else:
            transfer = check_transfer(begin_idx, (begin_idx + end_idx) // 2, records, visits) \
                       + transfer \
                       + check_transfer((begin_idx + end_idx) // 2, end_idx, records, visits)
            return transfer
    visits = {}
    tech_transfer_records = check_transfer(begin_idx, end_idx, homepage_snapshots, visits)
    tech_transfer_records = [(begin_ts, visits[begin_ts])] + tech_transfer_records + [(end_ts, visits[end_ts])]
    periods = [{
        "startTS": tech_transfer_records[i][0],
        "endTS": tech_transfer_records[i+1][0],
        "tech": tech_transfer_records[i][1]
    } for i in range(0, len(tech_transfer_records), 2)]
    return periods
    # for tech_transfer in tech_transfer_records:


def collect_tech_change_sites():
    """
    Entry func
    Collect site's (subhosts) tech change time range 
    """
    subhosts = db.site_tech.find({"techs": {"$exists": False}})
    subhosts = list(subhosts)
    print("Total:", len(subhosts))
    for i, subhost in enumerate(subhosts):
        print(i, subhost["_id"])
        period = subhost_tech_change(subhost['_id'])
        if len(period) == 0:
            continue
        db.site_tech.update_one({"_id": subhost['_id']}, {"$set": {"techs": period}})
        # print(json.dumps(period, indent=2))

collect_tech_change_sites()
    