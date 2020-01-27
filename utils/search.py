"""
Utils library for search
"""
import requests
import json
import sys
from bs4 import BeautifulSoup

sys.path.append('../')
import config
from utils import text_utils

google_query_dict = {
    "q": None,
    "key" : config.SEARCH_KEY,
    "cx" : config.SEARCH_CX
}

bing_query_dict = {
    "q": None
}

requests_header = {'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}
headers = {"Ocp-Apim-Subscription-Key": '978290f3b37c48538596753b4d2be65f'}

google_url = 'https://www.googleapis.com/customsearch/v1'
bing_url = 'https://api.cognitive.microsoft.com/bing/v7.0/search'


def get_headers(html):
    soup = BeautifulSoup(html, 'html.parser')
    possible = []
    title = soup.find('title')
    title = title.text if title.title != 'Wayback Machine' else ""
    for i in range(1, 7):
        tags = soup.find_all('h' + str(i))
        for tag in tags:
            if tag.text != "" and "Wayback Machine" not in tag.text: 
                if tag.text in title:               
                    return tag.text
                else:
                    possible.append(tag.text)
    return possible[0]


def google_search(query, end=0, param_dict={}):
    """
    Search using google
    """
    google_query_dict['q'] = query
    google_query_dict.update(param_dict)
    try:
        r = requests.get(google_url, params=google_query_dict)
        r = r.json()
    except Exception as e:
        print(str(e))
        return []
    if "items" not in r:
        return []
    end = len(r['items']) if end == 0 else min(len(r["items"]), end)
    return [ u["link"] for u in r['items'][:end]]


def bing_search(query, end=0):
    """
    Search using bing
    """
    bing_query_dict["q"] = query
    try:
        r = requests.get(bing_url, params=bing_query_dict, headers=headers)
        r = r.json()
    except Exception as e:
        print(str(e))
        return []
    if "webPages" not in r or 'value' not in r['webPages']:
        return []
    values = r["webPages"]['value']
    end = len(values) if end == 0 else min(len(values), end)
    return [u['url'] for u in values[:end]]