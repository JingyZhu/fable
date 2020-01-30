"""
    Counts the data of different urls status and correcsponds hosts
"""
from pymongo import MongoClient
import pymongo
import sys, os
import re
import json
from urllib.parse import urlparse
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import whois
import datetime
import numpy as np
import json

sys.path.append('../')
import config
from utils import plot, url_utils

db = MongoClient(config.MONGO_HOSTNAME).web_decay
# db = MongoClient(config.MONGO_HOSTNAME).test
year = 2019

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


def create_url_status_implicit_broken():
    """
    Creating collection for url_status_implicit_broken
    """
    urls = db.host_sample.aggregate([
        {"$lookup": {
            "from": "url_status",
            "let": {"hostname": "$hostname", "year": "$year"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$hostname", "$$hostname"]},
                    {"$eq": ["$year", "$$year"]}
                ]}}}
            ],
            "as": "in_sample"
        }},
        {"$match": {"in_sample.0": {"$exists": True}}},
        {"$unwind": "$in_sample"},
        {"$replaceRoot": { "newRoot": "$in_sample"} }
    ])
    db.url_status_implicit_broken.insert_many(list(urls), ordered=False)


def broken_200_breakdown_host():
    """
    Calculate the fraction of "real broken" breakdown hosts
    Should be run after implicit_broken.py/calculate_broken() to have similarity field in url_status
    """
    total = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True}, "year": year}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    total = list(total)[0]['count']
    
    match = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True}, "year": year}},
        {"$match": {"similarity": {"$gte": 0.8}}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    match = list(match)[0]['count']
    
    broken = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True}, "year": year}},
        {"$match": {"similarity": {"$lte": 0.2}}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    broken = list(broken)[0]['count']
    
    unsure = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True}, "year": year}},
        {"$match": {"similarity": {"$gt": 0.2, "$lt": 0.8}}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    unsure = list(unsure)[0]['count']
    print(match/total, broken/total, unsure/total)


def broken_200_breakdown_links():
    """
    Calculate the fraction of "real broken" breakdown hosts
    Should be run after implicit_broken.py/calculate_broken() to have similarity field in url_status
    """
    total = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True}, "year": year}},
        {"$count": "count"}
    ])
    total = list(total)[0]['count']
    
    match = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True}, "year": year}},
        {"$match": {"similarity": {"$gte": 0.8}}},
        {"$count": "count"}
    ])
    match = list(match)[0]['count']
    
    broken = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True}, "year": year}},
        {"$match": {"similarity": {"$lte": 0.2}}},
        {"$count": "count"}
    ])
    broken = list(broken)[0]['count']
    
    unsure = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True}, "year": year}},
        {"$match": {"similarity": {"$gt": 0.2, "$lt": 0.8}}},
        {"$count": "count"}
    ])
    unsure = list(unsure)[0]['count']


    print(match/total, broken/total, unsure/total)


def total_broken_host():
    """
    Total broken rate, catagorized by:
    Broken (4/5xx, DNSError, OtherError)
    Not Broken (Content match)
    Not sure (Landing Pages, content not match)
    """
    total = db.url_status_implicit_broken.aggregate([
        {"$match": {"year": year}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    total = list(total)[0]['count']

    broken = db.url_status_implicit_broken.aggregate([
        {"$match": {"year": year,  "$or": [
            {"status": re.compile("^([45]|DNSError|OtherError)")},
            {"similarity": {"$exists": True, "$lte": 0.2}}
        ]}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    broken = list(broken)[0]['count']

    fine = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True, "$gte": 0.8}, "year": year}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    fine = list(fine)[0]['count']

    not_sure = db.url_status_implicit_broken.aggregate([
        {"$match": {"year": year, "$or": [
            {"similarity": {"$exists": True, "$gt": 0.2, "$lt": 0.8}},
            {"similarity": {"$exists": False}, "status": re.compile("^[2]")}
        ]}},
        {"$group": {"_id": "$hostname"}},
        {"$count": "count"}
    ])
    not_sure = list(not_sure)[0]['count']
    print(broken / total, fine / total, not_sure / total)


def total_broken_link():
    """
    Total broken rate, catagorized by:
    Broken (4/5xx, DNSError, OtherError)
    Not Broken (Content match)
    Not sure (Landing Pages, content not match)
    """
    total = db.url_status_implicit_broken.aggregate([
        {"$match": {"year": year}},
        {"$count": "count"}
    ])
    total = list(total)[0]['count']

    broken = db.url_status_implicit_broken.aggregate([
        {"$match": {"year": year,  "$or": [
            {"status": re.compile("^([45]|DNSError|OtherError)")},
            {"similarity": {"$exists": True, "$lte": 0.2}}
        ]}},
        {"$count": "count"}
    ])
    broken = list(broken)[0]['count']

    fine = db.url_status_implicit_broken.aggregate([
        {"$match": {"similarity": {"$exists": True, "$gte": 0.8}, "year": year}},
        {"$count": "count"}
    ])
    fine = list(fine)[0]['count']

    not_sure = db.url_status_implicit_broken.aggregate([
        {"$match": {"year": year, "$or": [
            {"similarity": {"$exists": True, "$gt": 0.2, "$lt": 0.8}},
            {"similarity": {"$exists": False}, "status": re.compile("^[23]")}
        ]}},
        {"$count": "count"}
    ])
    not_sure = list(not_sure)[0]['count']

    print(broken / total, fine / total, not_sure / total)
# create_host_status()


def status_200_broken_frac_host():
    """
    Calculate fraction of broken links for different 200 details
    Only for urls with similarity
    """
    data = [[] for _ in range(3)] # status, frac
    no_redirection = db.url_status_implicit_broken.aggregate([
        {"$match": {"year": year, "similarity": {"$exists": True}, "detail": "no redirection"}},
    ])
    length, broken, good, unsure = 0, 0, 0, 0
    for length, url in enumerate(no_redirection):
        if url['similarity'] >= 0.8: good += 1
        elif url['similarity'] <= 0.2: broken += 1
        else: unsure += 1
    length += 1
    print(broken / length, unsure / length, good / length)
    data[0] = [broken / length, unsure / length, good / length]

    homepage = db.url_status_implicit_broken.aggregate([
        {"$match": {"year": year, "similarity": {"$exists": True}, "detail": "homepage redirection"}}
    ])
    length, broken, good, unsure = 0, 0, 0, 0
    for length, url in enumerate(homepage):
        if url['similarity'] >= 0.8: good += 1
        elif url['similarity'] <= 0.2: broken += 1
        else: unsure += 1
    length += 1
    print(broken / length, unsure / length, good / length)
    data[1] = [broken / length, unsure / length, good / length]

    non_homepage = db.url_status_implicit_broken.aggregate([
        {"$match": {"year": year, "similarity": {"$exists": True}, "detail": "non-home redirection"}}
    ])
    length, broken, good, unsure = 0, 0, 0, 0
    for length, url in enumerate(non_homepage):
        if url['similarity'] >= 0.8: good += 1
        elif url['similarity'] <= 0.2: broken += 1
        else: unsure += 1
    length += 1
    print(broken / length, unsure / length, good / length)
    data[2] = [broken / length, unsure / length, good / length]
    
    return data


def status_200_broken_frac_link():
    """
    Calculate fraction of broken links for different 200 details
    Only for urls with similarity
    Plot stacked bar plots

    Return: data[0]: noredir, data[1]: homepage, data[2]: non-homepage
    """
    global year
    prev_year = year
    years = [1999, 2004, 2009, 2014, 2019]
    no_redir, homepage, nonhome = None, None, None

    for year in years:
        no_redirection = db.url_status_implicit_broken.aggregate([
            {"$match": {"year": year, "similarity": {"$exists": True}, "detail": "no redirection"}}
        ])
        length, broken, good, unsure = 0, 0, 0, 0
        for length, url in enumerate(no_redirection):
            if url['similarity'] >= 0.8: good += 1
            elif url['similarity'] <= 0.2: broken += 1
            else: unsure += 1
        length += 1
        print(broken / length, unsure / length, good / length)
        datus = [[broken / length], [unsure / length], [good / length]]
        no_redir = np.array(datus) if no_redir is None else np.hstack((no_redir, datus))

        homepage_redir = db.url_status_implicit_broken.aggregate([
            {"$match": {"year": year, "similarity": {"$exists": True}, "detail": "homepage redirection"}}
        ])
        length, broken, good, unsure = 0, 0, 0, 0
        for length, url in enumerate(homepage_redir):
            if url['similarity'] >= 0.8: good += 1
            elif url['similarity'] <= 0.2: broken += 1
            else: unsure += 1
        length += 1
        print(broken / length, unsure / length, good / length)
        datus = [[broken / length], [unsure / length], [good / length]]
        homepage = np.array(datus) if homepage is None else np.hstack((homepage, datus))

        non_homepage = db.url_status_implicit_broken.aggregate([
            {"$match": {"year": year, "similarity": {"$exists": True}, "detail": "non-home redirection"}}
        ])
        length, broken, good, unsure = 0, 0, 0, 0
        for length, url in enumerate(non_homepage):
            if url['similarity'] >= 0.8: good += 1
            elif url['similarity'] <= 0.2: broken += 1
            else: unsure += 1
        length += 1
        print(broken / length, unsure / length, good / length)
        datus = [[broken / length], [unsure / length], [good / length]]
        nonhome = np.array(datus) if nonhome is None else np.hstack((nonhome, datus))
    
    years = [str(y) for y in years]
    year = prev_year
    stackname = ['broken', 'unsure', 'good']

    plot.plot_stacked_bargroup(no_redir, xname=years, stackname=stackname, show=False)
    plt.ylabel('Fraction')
    plt.title('Breakdown for no redirection urls')
    plt.savefig('fig/noredir_links.png')
    plt.close()
    plot.plot_stacked_bargroup(homepage, xname=years, stackname=stackname, show=False)
    plt.ylabel('Fraction')
    plt.title('Breakdown for homepage redirection urls')
    plt.savefig('fig/homepage_links.png')
    plt.close()
    plot.plot_stacked_bargroup(nonhome, xname=years, stackname=stackname, show=False)
    plt.ylabel('Fraction')
    plt.title('Breakdown for non-homepage redirection urls')
    plt.savefig('fig/nonhome_links.png')
    plt.close()


def dirname_fate():
    """
    See the fate of each directories
    Only counts dirname with >= 3 samples
    """
    fate = defaultdict(dict)
    hosts = db.url_status_implicit_broken.aggregate([
        {"$group": {"_id": "$hostname"}}
    ])
    for i, host in enumerate(list(hosts)):
        if i % 100 == 0: print(i)
        host_fate = defaultdict(list)
        urls = db.url_status_implicit_broken.find({"hostname": host['_id']})
        for url in urls:
            up = urlparse(url['url'])
            subhost, path, query = up.netloc, up.path, up.query
            if path == '': path = '/'
            dirname = os.path.dirname(path)
            host_fate["{}|{}".format(subhost, dirname)].append(url_utils.status_categories(url['status'], url['detail']))
        dir_count = {}
        for dirname, statuss in host_fate.items():
            if len(statuss) >= 3:
                dir_count[dirname] = set(statuss)
        fate[host['_id']] = dir_count
    fate_list = []
    for hostname, dir_count in fate.items():
        for dirname, statuss in dir_count.items():
            fate_list.append(len(statuss))
    fate = {h: {d: list(s) for d, s in ds.items() if len(s) > 1} for h, ds in fate.items()}
    fate = {h: ds for h, ds in fate.items() if len(ds) > 0}
    json.dump(fate, open('fate.json', 'w+'))
    print(len(fate_list))
    plot.plot_CDF([fate_list], show=False)
    plt.xlabel("# status")
    plt.ylabel("CDF across (subhost, path_dir)")
    plt.title("Whether url with same dirname share the fate")
    plt.savefig('fig/fate.png')



if __name__ == '__main__':
    years = [1999, 2004, 2009, 2014, 2019]
    # for year in years:
    #     total_broken_host()
    # for year in years:
    #     total_broken_link()
    dirname_fate()
    
