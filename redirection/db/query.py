import pymongo
from pymongo import MongoClient
import json, csv

client = MongoClient()
db = client.web_decay

# missing_url = []
# for obj in db.redirection.find({'similarity': {'$lt': 0.4}}):
#     missing_url.append(obj['url'])

# json.dump(missing_url, open('../search/missing_url.json', 'w+'))

rval = []

for obj in db.search.find():
    url = obj['url']
    wayback_url = db.redirection.find_one({'url': url})['wayback_url']
    output_obj = {
        "url": url,
        "titlematch_url" : obj['titlematch_url'] if 'titlematch_url' in obj else "N/A",
        "titlematch_simi" : obj['titlematch_similarity'] if 'titlematch_similarity' in obj else 0,
        "topN_url" : obj['topN_url'] if 'topN_url' in obj else "N/A",
        "topN_simi" : obj['topN_similarity'] if "topN_similarity" in obj else 0,
        "wayback_url": wayback_url
    }
    rval.append(output_obj)

csvwriter = csv.DictWriter(open('relocate.csv', 'w+'), fieldnames=rval[0].keys())

csvwriter.writeheader()
csvwriter.writerows(rval)
   
