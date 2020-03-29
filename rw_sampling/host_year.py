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
db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
proxies = config.PROXIES[1]
year = 2019

metadata = json.load(open('hosts_year_10k.json', 'r'))

def wayback_earliest_year(hostname):
    """
    Get the earliest year this hostname has crawl in wayback machine
    """
    url = "*.{}/*".format(hostname)
    params = {
        "limit": 10,
        "collapse": 'timestamp:4'
    }
    rval, _ = crawl.wayback_index(url, param_dict=params, proxies=proxies)
    if len(rval) > 0:
        return int(rval[0][0][:4])
    else:
        return 2020


def get_links():
    """
    Get links by calling CDX API 
    Only newly crawled links will be consider links created in that year
    """
    # Prevent same obj being insert multiple times
    db.url_year_added.create_index('hostname')
    db.url_population.create_index('hostname')
    db.hosts_added_links.create_index([('hostname', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
    keys = db.hosts_meta.find({'year': year})
    keys = list([k['hostname'] for k in keys])
    idx, length = config.HOSTS.index(socket.gethostname()), len(keys)
    key_shards = sorted(keys)[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    # key_shards = sorted(keys[:100])
    for i, hostname in enumerate(key_shards):
        print(i, hostname)
        crawled_hosts = list(db.hosts_added_links.find({'hostname': hostname}))
        if len(crawled_hosts) > 0:
            early_year = max([c['year'] for c in crawled_hosts]) + 1
            existed_urls = set([u['url'] for u in list(db.url_year_added.find({'hostname': hostname}))])
        else:
            existed_urls = set()
            early_year = wayback_earliest_year(hostname)
        data = crawl.wayback_year_links('*.{}/*'.format(hostname), list(range(early_year, year + 1)))
        objs = []
        for y in sorted(data.keys()):
            urls = data[y]
            for url in urls:
                if url not in existed_urls:
                    objs.append({
                        "url": url,
                        "hostname": hostname,
                        "year": y
                    })
                    existed_urls.add(url)
            if len(objs) > 0:
                try:
                    db.hosts_added_links.insert_one({
                        "hostname": hostname,
                        "year": y,
                        "added_links": len(objs)
                    })
                    db.url_year_added.insert_many(objs, ordered=False)
                except Exception as e:
                    print(str(e))
            if y == year and len(objs) >= 100: # The year for sampling
                try:
                    db.url_population.insert_many(objs, ordered=False)
                except:
                    pass
            objs = []


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
    default_func = 'get_links'
    calling_dict = {name: var for name, var in locals().items() if inspect.isfunction(var)}
    func = default_func if len(sys.argv) < 2 else sys.argv[1]
    calling_dict[func]()