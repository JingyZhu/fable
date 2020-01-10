"""
    Counts the data of different urls status and correcsponds hosts
"""
from pymongo import MongoClient
import pymongo
import sys
import re
import json
from urllib.parse import urlparse
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import whois
import datetime

sys.path.append('../')
import config
from utils import plot, url_utils

db = MongoClient(config.MONGO_HOSTNAME).web_decay
# db = MongoClient(config.MONGO_HOSTNAME).test
year = 2014

def create_host_status():
    """
    Create collections as year, host, status (one KXX), and detail
    """
    db.host_status.create_index([('hostname', pymongo.ASCENDING), ('year', pymongo.ASCENDING),\
         ('status', pymongo.ASCENDING), ('detail', pymongo.ASCENDING)], unique=True)
    for url in db.url_status.find({'year': year}):
        status = url['status']
        detail = url['detail']
        status = str(int(status) // 100) + "xx" if status != 'DNSError' and status != 'OtherError' else status
        if not re.compile("^([2345]|DNSError|OtherError)").match(status): continue
        try:
            db.host_status.insert_one({
                "hostname": url['hostname'],
                "year": year,
                "status": status,
                'detail': detail
            })
        except:
            continue


def create_subhost_status():
    """
    Create collections as year, subhost, host, status (one KXX), and detail
    """
    db.subhost_status.create_index([('subhostname', pymongo.ASCENDING), ('year', pymongo.ASCENDING),\
         ('status', pymongo.ASCENDING), ('detail', pymongo.ASCENDING)], unique=True)
    for url in db.url_status.find({'year': year}):
        status = url['status']
        detail = url['detail']
        status = str(int(status) // 100) + "xx" if status != 'DNSError' and status != 'OtherError' else status
        if not re.compile("^([2345]|DNSError|OtherError)").match(status): continue
        try:
            db.host_status.insert_one({
                "subhostname": urlparse(url['url']).netloc,
                "hostname": url['hostname'],
                "year": year,
                "status": status,
                'detail': detail
            })
        except:
            continue



def status_breakdown_host():
    total_host = db.host_status.aggregate([
        {"$match": {"year": year}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    total_host = list(total_host)[0]['count']
    error_host = db.host_status.aggregate([
        {"$match": {"year": year, "status": re.compile('^[45]')}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    error_host = list(error_host)[0]['count']
    no_redirection_host = db.host_status.aggregate([
        {"$match": {"year": year, "detail": "no redirection"}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    no_redirection_host = list(no_redirection_host)[0]['count']
    home_redirction_host = db.host_status.aggregate([
        {"$match": {"year": year, "detail": "homepage redirection"}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    home_redirction_host = list(home_redirction_host)[0]['count']
    nonhome_redirction_host = db.host_status.aggregate([
        {"$match": {"year": year, "detail": "non-home redirection"}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    nonhome_redirction_host = list(nonhome_redirction_host)[0]['count']
    dns_host = db.host_status.aggregate([
        {"$match": {"year": year, "status": "DNSError"}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    dns_host = list(dns_host)[0]['count']
    ping_error = db.host_status.aggregate([
        {"$match": {"year": year, "status": "OtherError", "detail": re.compile("^Ping")}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    ping_error = list(ping_error)[0]['count']
    not_open = db.host_status.aggregate([
        {"$match": {"year": year, "status": "OtherError", "detail": re.compile("^80_(?!open)\w+_443_(?!open)\w+")}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    not_open = list(not_open)[0]['count']
    some_open = db.host_status.aggregate([
        {"$match": {"year": year, "status": "OtherError", "detail": re.compile("^80_open_443_\w+|80_\w+_443_open")}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    some_open = list(some_open)[0]['count']
    print(error_host/total_host, no_redirection_host/total_host, home_redirction_host/total_host,\
            nonhome_redirction_host/total_host, dns_host/total_host,ping_error/total_host, not_open/total_host,\
            some_open/total_host)


def status_breakdown_links():
    total_links = db.url_status.aggregate([
        {"$match": {"year": year}},
        {"$count": "count"}
    ])
    total_links = list(total_links)[0]['count']
    error_links = db.url_status.aggregate([
        {"$match": {"year": year, "status": re.compile('^[45]')}},
        {"$count": "count"}
    ])
    error_links = list(error_links)[0]['count']
    no_redirection_links = db.url_status.aggregate([
        {"$match": {"year": year, "detail": "no redirection"}},
        {"$count": "count"}
    ])
    no_redirection_links = list(no_redirection_links)[0]['count']
    home_redirction_links = db.url_status.aggregate([
        {"$match": {"year": year, "detail": "homepage redirection"}},
        {"$count": "count"}
    ])
    home_redirction_links = list(home_redirction_links)[0]['count']
    nonhome_redirction_links = db.url_status.aggregate([
        {"$match": {"year": year, "detail": "non-home redirection"}},
        {"$count": "count"}
    ])
    nonhome_redirction_links = list(nonhome_redirction_links)[0]['count']
    dns_links = db.url_status.aggregate([
        {"$match": {"year": year, "status": "DNSError"}},
        {"$count": "count"}
    ])
    dns_links = list(dns_links)[0]['count']
    ping_error_links = db.url_status.aggregate([
        {"$match": {"year": year, "status": "OtherError", "detail": re.compile("^Ping")}},
        {"$count": "count"}
    ])
    ping_error_links = list(ping_error_links)[0]['count']
    not_open_links = db.url_status.aggregate([
        {"$match": {"year": year, "status": "OtherError", "detail": re.compile("^80_(?!open)\w+_443_(?!open)\w+")}},
        {"$count": "count"}
    ])
    not_open_links = list(not_open_links)[0]['count']
    some_open_links = db.url_status.aggregate([
        {"$match": {"year": year, "status": "OtherError", "detail": re.compile("^80_open_443_\w+|80_\w+_443_open")}},
        {"$count": "count"}
    ])
    some_open_links = list(some_open_links)[0]['count']
    print(error_links/total_links, no_redirection_links/total_links, home_redirction_links/total_links,\
            nonhome_redirction_links/total_links, dns_links/total_links, ping_error_links/total_links,\
                not_open_links/total_links, some_open_links/total_links)


def count_dnserror_subhost():
    """
    For each host that has not only DNSError, 
    see whether other status are result in different subhosts.
    And define there relationships
    """
    hosts = db.host_status.aggregate([
        {"$match": {"status": "DNSError", "year": year}},
        {"$lookup": {
            "from": "host_status",
            "localField": "hostname",
            "foreignField": "hostname",
            "as": "all_status"
        }},
        {"$project":{
            "hostname": "$hostname",
            "status": "$status",
            "_id": False,
            "all_status": "$all_status.status"
        }},
        {"$match": {"all_status.1": {"$exists": True}} },
    ])
    hosts = [h['hostname'] for h in hosts]
    netloc_status = defaultdict(lambda: defaultdict(set))
    for host in hosts:
        urls = db.url_status.find({"year": year, "hostname": host})
        for url in urls:
            netloc = urlparse(url['url']).netloc
            status = url_utils.status_categories(url['status'], url['detail'])
            netloc_status[host][netloc].add(status)
    netloc_status = {k: {kk: list(vv) for kk, vv in netloc_status[k].items()} for k in netloc_status}
    json.dump(netloc_status, open('dns_more.json', 'w+'))


def other_error():
    hosts = db.host_status.aggregate([
        {"$match": {"status": "OtherError", "year": year}},
        {"$group": {
            "_id": "$hostname",
            "errors": {"$push": "$detail"}
        }}
    ])
    hosts = list(hosts)
    # print(hosts)
    for obj in hosts:
        obj['errors'] = list(set(obj['errors']))
    json.dump(hosts, open('other_error.json', 'w+'))


def frac_45xx_links():
    missing_count = db.url_status.aggregate([
        {"$match": {"year": year, "status": re.compile("^[45]")}},
        {"$group": {"_id": "$hostname", "total": {"$sum": 1}}},
        {"$sort": {"total": 1}}
    ])
    missing_count = list([m['total'] for m in missing_count])
    plot.plot_CDF([missing_count], show=False)
    plt.xlabel("#urls has 4/5xx")
    plt.ylabel("CDF across hosts")
    plt.title("#urls has 4/5xx status code for hosts in 4/5xx")
    plt.savefig('fig/45xx_frac.png')


def ping_error_detail():
    """
        Dive deep into the ping error hosts
        For each subhost (with only ping error), check other subhost in the host
        If others have available status (2/3/4/5), probably DNS handling problem
        Else Probably the site is abandoned

        TODO Make it a database version
    """
    ping_hosts = list(db.host_status.find({"year": year, "detail": re.compile("^Ping")}))
    print(len(ping_hosts))
    ping_dict = {"avail": {}, "unavail": {}}
    for host in ping_hosts:
        d = defaultdict(set)
        netloc_stats = defaultdict(set)
        urls = db.url_status.find({"year": year, "hostname": host['hostname']})
        for url in urls:
            netloc = urlparse(url['url']).netloc
            if not re.compile("^([2345]|DNSError|OtherError)").match(url['status']): continue
            if re.compile("[2345]").match(url['status']):
                status = "avail"
            elif re.compile('^Ping').match(url['detail']):
                status = "ping"
            else:
                status = "unavail"
            d[netloc].add(status)
            netloc_stats[netloc].add(url['status'])
        valid, avail = False, False
        valid_netloc, avail_netloc = "", []
        for netloc, statuses in d.items():
            if "ping" in statuses and len(statuses) <= 1:
                valid = True
                valid_netloc = netloc
            if "avail" in statuses:
                avail = True
                avail_netloc.append(netloc)
        if valid:
            if avail:
                ping_dict['avail'][host['hostname']] = {
                    "ping error": valid_netloc,
                    "avail": [[n, list(netloc_stats[n])] for n in avail_netloc]
                }
            else:
                ping_dict['unavail'][host['hostname']] = [valid_netloc]
    print(len(ping_dict['avail']), len(ping_dict['unavail']))
    json.dump(ping_dict, open("ping_detail.json", 'w+'))


def whois_expiration():
    """
        For each host with only ping error and no other avail,
        Use whois to get the expiration data (if applicable)
    """
    data = json.load(open("ping_detail.json", 'r'))['unavail']
    print(len(data))
    last_updates = {}
    none_count, no_lu = 0, 0
    for i, host in enumerate(data):
        print(i, host)
        try:
            lu = whois.query(host).last_updated
            if lu: 
                last_updates[host] = lu.year
            else: 
                no_lu += 1
        except:
            none_count += 1
    print(none_count, no_lu)
    print(len(list(filter(lambda x: x >= 2019, last_updates.values()))))
    json.dump(last_updates, open('whois.json', 'w+'))


# ping_error_detail()
# whois_expiration()

# create_host_status()
year = 1999
status_breakdown_host()
year = 2004
status_breakdown_host()
year = 2009
status_breakdown_host()
year = 2014
status_breakdown_host()
year = 1999
status_breakdown_links()
year = 2004
status_breakdown_links()
year = 2009
status_breakdown_links()
year = 2014
status_breakdown_links()
