from pymongo import MongoClient
import json

db = MongoClient().web_decay

def update_wayback_urls_from_timestamp():
    wayback_home = 'http://web.archive.org/web/'
    redirection = db.redirection
    for obj in list(redirection.find()):
        url = obj['url']
        timestamp = obj['timestamp']
        wayback_url = wayback_home + timestamp + '/' + url
        redirection.find_one_and_update({'url': url}, {'$set': {'wayback_url': wayback_url}})
    

def add_match_to_urls():
    data = json.load(open('../manual_check/label.json', 'r'))
    redirection = db.redirection
    for obj in data:
        url = obj['url']
        match = obj['label']
        redirection.find_one_and_update({'url': url}, {'$set': {'match': match}})

    
add_match_to_urls()