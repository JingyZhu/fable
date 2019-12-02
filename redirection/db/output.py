"""
Output query to csv
"""
from pymongo import MongoClient
import csv

db = MongoClient().web_decay

def dict_2_csv(obj, output_file):
    fieldname = list(obj[0].keys())
    csvwriter = csv.DictWriter(open(output_file, 'w+'), fieldnames=fieldname)
    csvwriter.writeheader()
    for v in obj:
        csvwriter.writerow(v)

obj = db.search.find({}, {"url": 1, "_id": 0, "topN_url": 1, "topN_similarity": 1, "titlematch_url": 1, "titlematch_similarity": 1})
obj = list(obj)

_ = [o.update({
    'wayback_url': db.redirection.find_one({'url': o['url']})['wayback_url']
    }) for o in obj]



dict_2_csv(obj, "../search/manual_missing.csv")