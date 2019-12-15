"""
For each year, Get hosts urls on wayback machine
"""
import json
import sys
from pymongo import MongoClient
import pymongo
import socket
import gc
import time
import threading
import queue

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


def links_added_by_year(thread_num=10):
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



if __name__ == '__main__':
    links_added_by_year(30)
