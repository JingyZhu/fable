"""
Measure the outlink differences between requests and chrome load;
Currently only support all links. TODO: Only extract text/html as outlinks
chrome load - requests load 
[0, 0, 0, 2, 0, 0, 5, 1, 9, 0, 0, 1, 1, 0, 0, 1, 3, 0, 0, 7, 0, 0, 0, 0, 0, 0, 1, 2, 6, 9, 3, 0, 0, 10, 0, 0, -5, 0, 0, 0, -160, 0, -1, 0, 9, 2, 9, 1, 8, 0, 5, 2, 38, 4]
"""
import requests
from subprocess import call
from pymongo import MongoClient
from bs4 import BeautifulSoup
import os
import json


def get_outlinks(html):
    soup = BeautifulSoup(html, 'html.parser')
    outlink_list = []
    for atag in soup.findAll('a'):
        try:
            link = atag['href']
        except:
            continue
        outlink_list.append(link)
    return outlink_list


def chrome_load(url):
    """
    Load a page using chrome
    return a url list 
    """
    try:
        call(['node', '../run.js', url], timeout=120)
    except:
        call(['pkill', 'Chromium'])
        return []
    if os.path.exists('temp.html'):
        html = open('temp.html', 'r').read()
        outlinks = get_outlinks(html)
        os.remove('temp.html')
        return outlinks
    else:
        return []


def requests_load(url):
    """
    request.get a page
    """
    try:
        r = requests.get(url)
    except:
        return []
    html = r.text
    outlinks = get_outlinks(html)
    return outlinks


def main():
    data = json.load(open('status_200.json', 'r'))
    diff_dict = {}
    for i, url in enumerate(data):
        print(i, url)
        outlink1 = chrome_load(url)
        outlink2 = requests_load(url)
        diff_dict[url] = len(outlink1) - len(outlink2)
    print(list(diff_dict.values()))

if __name__== "__main__":
    main()