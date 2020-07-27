import json
from collections import defaultdict
import sys, random
import pandas as pd
from urllib.parse import urlsplit, urlunsplit
sys.setrecursionlimit(1500)

sys.path.append('../../')
from utils import text_utils, crawl

site_lan = json.load(open('Site_language.json', 'r'))
sites = json.load(open('Broken_sites.json', 'r'))

for i, site in enumerate(sites):
    print(i, site)
    if site in site_lan and site_lan[site] is not None:
        continue
    site_url = f'http://{site}'
    html = crawl.requests_crawl(site_url)
    if html is None:
        site_lan[site] = "No html"
    else:
        try:
            lan = text_utils.lang_meta(html)
            if lan == 'en':
                title = text_utils.extract_title(html)
                if not title.isascii(): lan = None
            site_lan[site] = lan
        except Exception as e:
            print(str(e))
            site_lan[site] = None
    if i % 100 == 0:
        json.dump(site_lan, open('Site_language.json', 'w+'))



