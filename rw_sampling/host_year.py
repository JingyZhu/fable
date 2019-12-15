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


def links_added_by_year():
    db.added_links.create_index([('hostname', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)
    for i, (hostname, start_year) in enumerate(metadata.items()):
        existed_url = set()
        for year in range(int(start_year), 2020):
            start = time.time()
            new_url = 0
            for obj in db.url_year.find({'hostname': hostname, 'year': year}):
                url = obj['url']
                if url not in existed_url:
                    existed_url.add(url)
                    new_url += 1
            db.added_links.insert_one({
                'hostname': hostname,
                'year': year,
                'added_links': new_url
            })
            end = time.time()
            print(i, hostname, year, end - start)


if __name__ == '__main__':
    links_added_by_year()
