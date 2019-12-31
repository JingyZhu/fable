from pymongo import MongoClient
import sys
import json

sys.path.append('../')
import config

db = MongoClient(config.MONGO_HOSTNAME).web_decay
hosts = db.host_status.aggregate([
    {"$match": {"status": "DNSError"}},
    {"$lookup": {
        "from": "url_status",
        "localField": "hostname",
        "foreignField": "hostname",
        "as": "all_urls"
    }},
    {"$project":{
        "hostname": "$hostname",
        "status": "$status",
        "_id": False,
        "all_urls": "$all_urls"
    }},
    {"$limit": 10}
])
hosts = list(hosts)
print(hosts)
json.dump(hosts, open("test.json", 'w+'))

