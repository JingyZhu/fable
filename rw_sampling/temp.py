import json
from pymongo import MongoClient

year = 1999
data = json.load(open('checkpoints/hosts_{}.json'.format(year)))
db = MongoClient().web_decay

objs = []
for k in data.keys():
    objs.append({
        "hostname": k,
        "year": year
    })

try:
    db.hosts_meta.insert_many(objs, ordered=False)
except:
    pass