from matplotlib import pyplot as plt
import sys
from pymongo import MongoClient
import re

sys.path.append('../')
from utils import plot
import config 

db = MongoClient(config.MONGO_HOSTNAME).web_decay

def frac_45xx_links():
    years = [1999, 2004, 2009, 2014]
    missing_counts = []
    for year in years:
        missing_count = []
        hosts = db.url_status_consec.aggregate([
            {"$match": {"year": year}},
            {"$group": {"_id": "$hostname"}}
        ])
        for host in hosts:
            count = db.url_status_consec.count_documents({"year": year, "hostname": host['_id'], "status": re.compile('^[45]')})
            missing_count.append(count)
        missing_counts.append(missing_count)
    plot.plot_CDF(missing_counts, classname=[str(y) for y in years], show=False)
    plt.xlabel("#urls has 4/5xx")
    plt.ylabel("CDF across hosts")
    plt.title("#urls has 4/5xx status code")
    plt.savefig('fig/45xx_frac_consec.png')

def frac_DNS_links():
    years = [1999, 2004, 2009, 2014]
    missing_counts = []
    for year in years:
        missing_count = []
        hosts = db.url_status.aggregate([
            {"$match": {"year": year}},
            {"$group": {"_id": "$hostname"}}
        ])
        for host in hosts:
            count = db.url_status.count_documents({"year": year, "hostname": host['_id'], "status": re.compile('DNSError')})
            missing_count.append(count)
        missing_counts.append(missing_count)
    plot.plot_CDF(missing_counts, classname=[str(y) for y in years], show=False)
    plt.xlabel("#urls has DNSError")
    plt.ylabel("CDF across hosts")
    plt.title("#urls has DNSError")
    plt.savefig('fig/dns_frac.png')

frac_DNS_links()