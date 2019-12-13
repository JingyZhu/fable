"""
For each year, Get hosts urls on wayback machine
"""
import json
import sys
from pymongo import MongoClient
import pymongo
import socket

sys.path.append('../')
from utils import crawl

hosts = ['lions', 'pistons', 'wolverines', 'redwings']
db = MongoClient('lions.eecs.umich.edu').web_decay

# Prevent same obj being insert multiple times
db.url_year.create_index([('url', pymongo.ASCENDING), ('year', pymongo.ASCENDING)], unique=True)

metadata = json.load(open('hosts_year_10k.json', 'r'))

def get_links(interval=4):
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
            print([len(v) for v in data.values()])
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

get_links()
