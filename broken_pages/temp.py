from pymongo import MongoClient
import sys
import json
import re

sys.path.append('../')
import config
from utils import db_utils

db = MongoClient(config.MONGO_HOSTNAME).web_decay

hosts = json.load(open("1khosts.json", 'r'))
count = 0
for host in hosts: 
    added_links = db.hosts_added_links.find_one({"hostname": host, "year": 1999})['added_links']
    if added_links >= 500: count += 1

print(count)

