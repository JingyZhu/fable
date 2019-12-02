"""
Crawl all the pages in wayback_urls_sample.json
If crawl failed, value = None
Save it into origin_html_sample.json
"""
from subprocess import *
import threading
import time
import sys
from queue import Queue
from os.path import join
import time
import json
from urllib.parse import urlparse
import platform
import os

def main(data):
    count = 1
    if os.path.exists('origin_html_sample.json'):
        html_origin = json.load(open('origin_html_sample.json', 'r'))
    for url in data.keys():
        print(count, url)
        count += 1
        if url in html_origin:
            continue
        try:
            call(['node', '../run.js', url], timeout=120)
        except Exception as e:
            print(str(e))
            html_origin[url] = None
            # call(['pkill', 'node'])
            continue
        html = open('temp.html', 'r').read()
        # if status_code[0] == '3':
        #     parse_1, parse_2 = urlparse(open('temp_url', 'r').read()), urlparse(value['To'])
        #     if parse_1.netloc != parse_2.netloc or parse_1.path != parse_2.path:
        #         html_wayback[url] = html
        #         value['wayback_url'] = wayback_url
        #         data[url] = value
        #         break
        # elif status_code[0] == '2':
        html_origin[url] = html

        if count % 5 == 0:
            json.dump(html_origin, open('origin_html_sample.json', 'w+'))
        os.remove('temp.html')
    
    json.dump(html_origin, open('origin_html_sample.json', 'w+'))


if __name__ == '__main__':
    data = json.load(open('../wayback/wayback_urls_sample.json', 'r'))
    main(data)
