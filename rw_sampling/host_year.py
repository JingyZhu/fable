"""
For each year, Get hosts urls on wayback machine
"""
import json
import sys
from pymongo import MongoClient
import pymongo
import socket
import string
import time
import threading
import queue
import re
import math
import matplotlib.pyplot as plt
import inspect

sys.path.append('../')
from utils import crawl, plot
import config

hosts = ['lions', 'pistons', 'wolverines', 'redwings']
db = MongoClient(config.MONGO_HOSTNAME).web_decay



metadata = json.load(open('hosts_year_10k.json', 'r'))

def get_links(interval=1):
    """
    Get links by calling CDX API 
    Keys is sharded in hostname
    """
    # Prevent same obj being insert multiple times
    db.url_year.create_index([('url', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
    keys = list(metadata.keys())
    size = len(keys)
    index = hosts.index(socket.gethostname())
    key_shards = sorted(keys)[int(index/4 * size): int((index + 1)/4 * size)]
    # key_shards = sorted(keys[:100])
    for i, hostname in enumerate(key_shards):
        print(i, hostname)
        early_year =int(metadata[hostname])
        for year in range(early_year, 2020, interval):
            data = crawl.wayback_year_links('*.{}/*'.format(hostname), [y for y in range(year, year + interval) if y < 2020])
            print([(k, len(v)) for k, v in data.items()])
            objs = []
            for year, urls in data.items():
                for url in urls:
                    objs.append({
                        "url": url,
                        "hostname": hostname,
                        "year": year
                    })
            if len(objs) > 0:
                try:
                    db.url_year.insert_many(objs, ordered=False)
                except:
                    pass


def get_latest_year(interval=1):
    """
    Get the lastest year for each hosts that wayback has snapshot
    Kyes is sharded in hostname
    """
    db.latest_year.create_index([('hostname', pymongo.ASCENDING)], unique=True)
    keys = list(metadata.keys())
    size = len(keys)
    index = hosts.index(socket.gethostname())
    key_shards = sorted(keys)[int(index/4 * size): int((index + 1)/4 * size)]
    # key_shards = sorted(keys[:100])
    for i, hostname in enumerate(key_shards):
        print(i, hostname)
        early_year = int(metadata[hostname])
        for year in range(2019, early_year-1, -interval):
            can_break = False
            data = crawl.wayback_year_links('*.{}/*'.format(hostname), [y for y in range(year, year - interval, -1) if y >= early_year], \
                                            max_limit=1, param_dict={'filter': ['!statuscode:400', 'mimetype:text/html']})
            print([(k, len(v)) for k, v in data.items()])
            for year in sorted(data.keys(), reverse=True):
                if len(data[year]) > 0:
                    try:
                        db.latest_year.insert_one({
                            'hostname': hostname,
                            'year': year
                        })
                        can_break = True
                        break
                    except:
                        pass
            if can_break:
                break


def links_added_by_year(thread_num=3):
    """
    Process the url_year data, deduplicate the obj based on years
    Only new added links in certain years will be shown
    Sharded the data into multiple scans (0-9, a-z)
    Expected average memory usage of 5GB per scan
    """
    db.url_year_added.create_index([('url', pymongo.ASCENDING)], unique=True)
    db.added_links.create_index([('hostname', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
    patterns = [c for c in string.ascii_lowercase] + ['0-9']
    def thread_func(q_in):
        while not q_in.empty():
            p = q_in.get()
            begin = time.time()
            match = re.compile('^[{}]'.format(p), re.IGNORECASE)
            keys_dict = {}
            for i, obj in enumerate(db.url_year.find({'hostname': match})):
                hostname, url, year = obj['hostname'], obj['url'], int(obj['year'])
                keys_dict.setdefault(hostname, {})
                keys_dict[hostname].setdefault(url, year)
                if year < keys_dict[hostname][url]:
                    keys_dicts[hostname][url] = year
            mid = time.time()
            keys_years = {}
            batch = []
            for hostname, values in keys_dict.items():
                keys_years.setdefault(hostname, {})
                for url, year in values.items():
                    batch.append({
                        'url': url,
                        'hostname': hostname,
                        'year': year
                    })
                    keys_years[hostname].setdefault(year, 0)
                    keys_years[hostname][year] += 1
            if len(batch) > 0:
                try:
                    db.url_year_added.insert_many(batch, ordered=False)
                except:
                    pass
            batch = []
            for hostname, values in keys_years.items():
                for year, count in values.items():
                    batch.append({
                        'hostname': hostname,
                        'year': year,
                        'added_links': count
                    })
            if len(batch) > 0:
                try:
                    db.added_links.insert_many(batch, ordered=False)
                except:
                    pass
            end = time.time()
            print(i, "Scans", mid - begin, end - begin)
    q_in = queue.Queue()
    for p in patterns:
        q_in.put(p)
    pools = []
    for _ in range(thread_num):
        pools.append(threading.Thread(target=thread_func, args=(q_in, )))
        pools[-1].start()
    for t in pools:
        t.join()
        
    

def fix(thread_num=3):
    """
    Process the url_year data, deduplicate the obj based on years
    Only new added links in certain years will be shown
    Sharded the data into multiple scans (0-9, a-z)
    Expected average memory usage of 5GB per scan
    """
    db.added_links.create_index([('hostname', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
    patterns = [c for c in string.ascii_lowercase] + ['0-9']
    def thread_func(q_in):
        while not q_in.empty():
            p = q_in.get()
            begin = time.time()
            match = re.compile('^[{}]'.format(p), re.IGNORECASE)
            keys_dict = {}
            for i, obj in enumerate(db.url_year_added.find({'hostname': match})):
                hostname, url, year = obj['hostname'], obj['url'], int(obj['year'])
                keys_dict.setdefault(hostname, {})
                keys_dict[hostname].setdefault(url, year)
                if year < keys_dict[hostname][url]:
                    keys_dicts[hostname][url] = year
            mid = time.time()
            keys_years = {}
            for hostname, values in keys_dict.items():
                keys_years.setdefault(hostname, {})
                for url, year in values.items():
                    keys_years[hostname].setdefault(year, 0)
                    keys_years[hostname][year] += 1
            batch = []
            for hostname, values in keys_years.items():
                for year, count in values.items():
                    batch.append({
                        'hostname': hostname,
                        'year': year,
                        'added_links': count
                    })
            if len(batch) > 0:
                try:
                    db.added_links.insert_many(batch, ordered=False)
                except:
                    pass
            end = time.time()
            print(i, "Scans", mid - begin, end - begin)
    q_in = queue.Queue()
    for p in patterns:
        q_in.put(p)
    pools = []
    for _ in range(thread_num):
        pools.append(threading.Thread(target=thread_func, args=(q_in, )))
        pools[-1].start()
    for t in pools:
        t.join()


def plot_add_links():
    more = [{}, {}, {}, {}] #more than 10, 100, 1k and 10k
    exact = []
    appears = [{}, {}, {}, {}]
    for obj in db.added_links.find():
        hostname, year, added_links = obj['hostname'], int(obj['year']), int(obj['added_links'])
        end_idx = int(math.log10(added_links)) if int(math.log10(added_links)) <= 4 else 4
        for i in range(0, end_idx):
            more[i].setdefault(year, set())
            more[i][year].add(hostname)
            appears[i].setdefault(hostname, 0)
            appears[i][hostname] += 1
        exact.append(added_links)
    years = []
    for i in range(4):
        years += list(more[i].keys())
    years = sorted(list(set(years)))
    data = [[] for _ in range(4)]
    for year in years:
        for i in range(4):
            num_links = len(more[i][year]) if year in more[i] else 0
            data[i].append(num_links)
    plot.plot_bargroup(data, years, ['10', '100', '1k', '10k'], show=False)
    plt.ylabel('#hosts')
    plt.title('#Hosts with more than #added links in certain year')
    plt.show()
    # Plot CDF of #links across host*yuear
    plot.plot_CDF([exact], show=False)
    plt.xscale('log')
    plt.title('{Year, Host} #added links')
    plt.xlabel('#added links')
    plt.ylabel('CDF {year, host}')
    plt.show()
    # Plot overlap years
    appears = [list(a.values()) for a in appears]
    plot.plot_CDF(appears, classname=['10', '100', '1k', '10k'], show=False)
    plt.title('#years each hosts appears in each category')
    plt.ylabel('CDF of hosts')
    plt.xlabel('years')
    plt.show()



if __name__ == '__main__':
    default_func = 'plot_add_links'
    calling_dict = {name: var for name, var in locals().items() if inspect.isfunction(var)}
    func = default_func if len(sys.argv) < 2 else sys.argv[1]
    calling_dict[func]()