"""
Inspect temporal status (change) of sites.
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
        if begin_tech is None or end_tech is None: # Crawl failed
            print("crawl failed")
            return transfer
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
    random.shuffle(subhosts)
    print("Total:", len(subhosts))
    for i, subhost in enumerate(subhosts):
        print(i, subhost["_id"])
        period = subhost_tech_change(subhost['_id'])
        if len(period) == 0:
            continue
        db.site_tech.update_one({"_id": subhost['_id']}, {"$set": {"techs": period}})
        # print(json.dumps(period, indent=2))


def diff_dict(a, b):
    """Get Changes from a to b. Assum a, b are dict(list)"""
    changes = []
    addition_a = set(a.keys()) - set(b.keys())
    addition_b = set(b.keys()) - set(a.keys())
    changes = [('delete', aa, a[aa]) for aa in addition_a] + [('add', ab, b[ab]) for ab in addition_b]
    shared_keys = set(a.keys()).intersection(set(b.keys()))
    for k in shared_keys:
        add_a = set(a[k]) - set(b[k])
        add_b = set(b[k]) - set(a[k])
        changes += [('change', k, add_a, add_b)]
    rval = []
    for c in changes:
        if c[0] in ['delete', 'add']:
            rval += [(c[0], c[1], cc) for cc in c[2]]
        else:
            rval += [(c[0], c[1], before, after) for before, after in itertools.product(c[2], c[3])]
    return rval


def match_before_after_urls(query_meta, days):
    subhost = query_meta['subhost']
    delta = datetime.timedelta(days=days)
    before_range = [query_meta['beforeTS'] - delta, query_meta['beforeTS']]
    before_range = [t.strftime('%Y%m%d%H%M%S') for t in before_range]
    param_dict = {
        "from": before_range[0],
        "to": before_range[1],
        "filter": ['statuscode:200', 'mimetype:text/html']
    }
    urls, _ = crawl.wayback_index(subhost + '/*', param_dict=param_dict, proxies=PS.select())
    before_dict = {u[1]: u[0] for u in urls}

    after_range = [query_meta['afterTS'], query_meta['afterTS'] + delta]
    after_range = [t.strftime('%Y%m%d%H%M%S') for t in after_range]
    param_dict = {
        "from": after_range[0],
        "to": after_range[1],
        "filter": ['mimetype:text/html']
    }
    urls, _ = crawl.wayback_index(subhost + '/*', param_dict=param_dict, proxies=PS.select())
    urls = sorted(urls, key=lambda x: int(x[0]))
    after_dict = {}
    for url in urls: after_dict[url[1]] = [url[0], url[2]]
    intersect = set(before_dict.keys()).intersection(set(after_dict.keys()))
    final_dict = {}
    for url in intersect:
        final_dict[url] = [before_dict[url]] + after_dict[url]
    return final_dict


def collect_close_snapshots(days=30):
    """
    Entry func
    Collect urls appears in both before tech change and after tech change
    Collect urls appears in middle of no tech change
    days: Time threshold for tech changes & inspection windows of snapshots
    """
    site_with_techs = db.site_tech.find({"techs": {"$exists": True}})
    db.site_url_before_after.create_index([("subhost", pymongo.ASCENDING)])
    db.site_url_before_after.create_index([("url", pymongo.ASCENDING), ("type", pymongo.ASCENDING)], unique=True)
    tech_changes = []
    site_with_techs = list(site_with_techs)
    for site in site_with_techs:
        if db.site_url_before_after.find_one({"subhost": site['subhost'], "type": "Change"}): 
            continue
        for i in range(len(site['techs']) - 1):
            techs = site['techs']
            endTS = dparser.parse(techs[i]['endTS'])
            beginTS = dparser.parse(techs[i+1]['startTS'])
            diff_days = (beginTS - endTS).days
            if diff_days < 2*days:
                tech_changes.append({
                    "subhost": site['subhost'],
                    "beforeTS": endTS,
                    "afterTS": beginTS,
                    "beforeTech": techs[i],
                    "afterTech": techs[i+1],
                    "periodID": i
                })
    print("Total1:", len(tech_changes))
    for count, tech_change in enumerate(tech_changes):
        print(count, tech_change['subhost'])
        in_range_urls = match_before_after_urls(tech_change, days)
        in_range_urls = [{
            "url": u,
            "type": "Change",
            "subhost": tech_change['subhost'],
            "beforeTS": li[0],
            "afterTS": li[1],
            "afterStatus": li[2],
            "periodID": tech_change['periodID'],
            "beforeTech": tech_change['beforeTech'],
            "afterTech": tech_change['afterTech']
        } for u, li in in_range_urls.items()]
        try: db.site_url_before_after.insert_many(in_range_urls, ordered=False)
        except: print('db insert failed (Or insert list empty)')

    tech_sames = []
    for site in site_with_techs:
        if db.site_url_before_after.find_one({"subhost": site['subhost'], "type": "Same"}): 
            continue
        for i in range(len(site['techs'])):
            techs = site['techs']
            endTS = dparser.parse(techs[i]['endTS'])
            beginTS = dparser.parse(techs[i]['startTS'])
            diff_days = (endTS - beginTS).days
            if diff_days >= 3*days:
                delta = datetime.timedelta(days=days//2)
                middle = datetime.timedelta(days=diff_days//2)
                beginTS = beginTS + middle - delta
                endTS = endTS - middle + delta
                tech_sames.append({
                    "subhost": site['subhost'],
                    "beforeTS": beginTS,
                    "afterTS": endTS,
                    "periodID": i
                })
    print("Total2:", len(tech_sames))
    for count, tech_same in enumerate(tech_sames):
        print(count, tech_same['subhost'])
        in_range_urls = match_before_after_urls(tech_same, days)
        in_range_urls = [{
            "url": u,
            "type": "Same",
            "subhost": tech_same['subhost'],
            "beforeTS": li[0],
            "afterTS": li[1],
            "afterStatus": li[2],
            "periodID": tech_same['periodID']
        } for u, li in in_range_urls.items()]
        try: db.site_url_before_after.insert_many(in_range_urls, ordered=False)
        except: print('db insert failed')


def collect_changes():
    """
    Entry func
    Collect the frequency (count) of each type of change appear
    """
    urls = db.site_url_before_after.aggregate([
        {"$match": {"type": "Change"}},
        {"$group": {"_id":{
            "periodID": "$periodID",
            "subhost": "$subhost",
        }, "total": {"$sum": 1}}},
        {"$match": {"total": {"$gte": 50}}}
    ])
    changes_collect = defaultdict(int)
    urls = list(urls)
    print(len(urls))
    for url in urls:
        url['_id'].update({"type": "Change"})
        obj = db.site_url_before_after.find_one(url['_id'])
        changes = diff_dict(obj['beforeTech']['tech'], obj['afterTech']['tech'])
        for c in changes:
            changes_collect[c] += 1
    print(changes_collect)


if __name__ == '__main__':
    collect_close_snapshots()
    