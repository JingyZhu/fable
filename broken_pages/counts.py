"""
    Counts the data of different urls status and correcsponds hosts
"""
from pymongo import MongoClient
import pymongo
import sys
import re
import json
from urllib.parse import urlparse
from collections import defaultdict

sys.path.append('../')
import config
from utils import plot, url_utils

db = MongoClient(config.MONGO_HOSTNAME).web_decay
db = MongoClient(config.MONGO_HOSTNAME).test
year = 1999

def create_status_by_host():
    """
    Create collections as year, host, status (one KXX), and detail
    """
    db.host_status.create_index([('hostname', pymongo.ASCENDING), ('year', pymongo.ASCENDING),\
         ('status', pymongo.ASCENDING), ('detail', pymongo.ASCENDING)], unique=True)
    for url in db.url_status.find({'year': year}):
        status = url['status']
        status = str(int(status) // 100) + "xx" if status != 'DNSError' and status != 'OtherError' else status
        if not re.compile("^([2345]|DNSError|OtherError)").match(status): continue
        try:
            db.host_status.insert_one({
                "hostname": url['hostname'],
                "year": year,
                "status": status,
                'detail': url['detail']
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
    other_host = db.host_status.aggregate([
        {"$match": {"year": year, "status": "OtherError"}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    other_host = list(other_host)[0]['count']
    print(error_host/total_host, no_redirection_host/total_host, home_redirction_host/total_host,\
            nonhome_redirction_host/total_host, dns_host/total_host, other_host/total_host)


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
    other_links = db.url_status.aggregate([
        {"$match": {"year": year, "status": "OtherError"}},
        {"$count": "count"}
    ])
    other_links = list(other_links)[0]['count']
    print(error_links/total_links, no_redirection_links/total_links, home_redirction_links/total_links,\
            nonhome_redirction_links/total_links, dns_links/total_links, other_links/total_links)

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
    json.dump(netloc_status, open('test.json', 'w+'))

count_dnserror_subhost()
# create_status_by_host()
