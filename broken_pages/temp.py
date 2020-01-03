from pymongo import MongoClient
import sys
import json
import re

sys.path.append('../')
import config
from utils import db_utils

db = MongoClient(config.MONGO_HOSTNAME).web_decay
year = 1999

# hosts1 = list(db.url_sample.aggregate([{"$group": {"_id": "$hostname"}}]))
# hosts2 = list(db_utils.Hosts_gte_N_links_in_year(db, 500, 1999))

hosts_set = set()
urls = db.url_sample.find()
for url in urls:
    if not db.url_status.find_one({"_id": url['_id']}):
        db.url_sample.delete_many({"hostname": url["hostname"]})
        objs = list(db.url_status.find({"hostname": url["hostname"]}, {"status": False, "detail": False, "error_code": False}))
        try:
            db.url_sample.insert_many(objs)
        except:
            hosts_set.add(url['hostname'])
json.dump(list(hosts_set), open("wrong_sample.json", 'w+'))

