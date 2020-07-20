import json
from collections import defaultdict
import sys, random

import pandas as pd
import time
from bs4 import BeautifulSoup
from urllib.parse import urlsplit, urlunsplit, parse_qs

sys.path.append('../../')
from utils import crawl, text_utils, url_utils, sic_transit
import config
from pymongo import MongoClient

"""
See how many urls has canonical tag
"""
param_dict = {
    "filter": ['statuscode:200', 'mimetype:text/html'],
}

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').ReorgPageFinder

def most_related_url(url, cands):
    """Find url in cands with most overlapped query keys"""
    us = urlsplit(url)
    qs = parse_qs(us.query)
    max_overlap, max_url = 0, ''
    for cand in cands:
        cand_us = urlsplit(cand)
        cand_q = list(parse_qs(cand_us.query).keys())
        overlap = len(set(qs.keys()).intersection(cand_q))
        if overlap > max_overlap:
            max_overlap = overlap
            max_url = cand
    return max_url

def get_canonical(html):
    try:
        soup = BeautifulSoup(html, 'lxml')
        links = soup.find_all('link', {'rel': "canonical"})
        if len(links) > 0:
            return links[0]['href']
        else:
            return None
    except Exception as e:
        print(f'get_canonical: {str(e)}')
        return None

data = json.load(open('../../tmp/query.json', 'r'))
output = []
c = 0
for query in data:
    wayback_canonical = {}
    na_urls = db.na_urls.find_one({'url': query['url']})
    if na_urls and 'false_positive_broken' in na_urls:
        continue
    if not query['no_snapshot']:
        continue
    url = query['url']
    c += 1
    print(c, url)
    wayback_urls = query['wayback']
    most_simi_url = most_related_url(url, wayback_urls)
    wayback_canonical['url'] = most_simi_url
    print('most_related', most_simi_url)
    if most_simi_url == '':
        query.update({'wayback_canonical': wayback_canonical})
        output.append(query)
        continue
    sps, _ = crawl.wayback_index(most_simi_url, param_dict=param_dict, total_link=True)
    sps.sort(key=lambda x: int(x[0]), reverse=True)
    html = crawl.requests_crawl(sps[0][1])
    can = get_canonical(html)
    if can is not None:
        print('Canonical', can)
        wayback_canonical['canonical'] = can
    query.update({'wayback_canonical': wayback_canonical})
    output.append(query)

json.dump(output, open('query_wayback.json', 'w+'))
