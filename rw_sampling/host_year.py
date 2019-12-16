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

sys.path.append('../')
from utils import crawl

hosts = ['lions', 'pistons', 'wolverines', 'redwings']
db = MongoClient().web_decay



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


def links_added_by_year_backup(thread_num=10):
    db.url_year_added.create_index([('url', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
    db.added_links.create_index([('hostname', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
    def thread_func(q_in):
        while not q_in.empty():
            i, hostname, start_year = q_in.get()
            if int(start_year) > 2019:
                print(i, hostname, 'pass')
                continue
            url_match = {}
            year_count = {}
            start = time.time()
            for obj in db.url_year.find({'hostname': hostname}):
                url, year = obj['url'], int(obj['year'])
                if url not in url_match or url_match[url] > year:
                    url_match[url] = year
            for url, year in url_match.items():
                db.url_year_added.insert_one({
                    'url': url,
                    'hostname': hostname,
                    'year': year
                })
                year_count.setdefault(year, 0)
                year_count[year] += 1
            for year, count in year_count.items():
                db.added_links.insert_one({
                    'hostname': hostname,
                    'year': year,
                    'added_links': count
                })
            end = time.time()
            print(i, hostname, year, end - start)
    
    q_in = queue.Queue()
    for i, (hostname, start_year) in enumerate(metadata.items()):
        q_in.put((i, hostname, start_year))
    pools = []
    for _ in range(thread_num):
        pools.append(threading.Thread(target=thread_func, args=(q_in, )))
        pools[-1].start()
    for t in pools:
        t.join()


def links_added_by_year(thread_num=3):
    """
    Process the url_year data, deduplicate the obj based on years
    Only new added links in certain years will be shown
    Sharded the data into multiple scans (0-9, a-z)
    Expected average memory usage of 5GB per scan
    """
    db.url_year_added.create_index([('url', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
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



if __name__ == '__main__':
    fix()
