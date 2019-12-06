import json
from urllib.parse import urlparse

url_db = json.load(open('url_db_2017.json', 'r'))
hosts = json.load(open('hosts.json', 'r'))

urls = set() 
for obj in url_db:
    urls.add(urlparse(obj['url']).netloc) 

count = 0
for k in hosts:
    for w in urls:
        if k in w:
            print(k, w)
            count += 1
            break

print(count, len(hosts))

# 139 1016