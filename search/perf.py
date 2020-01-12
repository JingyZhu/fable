import brotli
import sys
import requests
import os
from os.path import join
import json
from subprocess import call
from pymongo import MongoClient
from bs4 import BeautifulSoup
import random

sys.path.append('../')
import config
from utils import text_utils

db = MongoClient(config.MONGO_HOSTNAME).web_decay

google_query_dict = {
    "q": None,
    "key" : "AIzaSyCq145QuNtIRGsluXo4n6lUbrvdOLA_hCY",
    "cx" : "006035121867626378213:tutaxpqyro8",
    # "fileType": "HTML"
}


requests_header = {'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}
headers = {"Ocp-Apim-Subscription-Key": '978290f3b37c48538596753b4d2be65f'}

bing_query_dict = {
    "q": None
}

google_url = 'https://www.googleapis.com/customsearch/v1'
bing_url = 'https://api.cognitive.microsoft.com/bing/v7.0/search'

#corpus = list(db.test_search.find({"content": {"$exists": True}}, {"content": True}))
#corpus = [e['content'] for e in corpus]
#TFidf = text_utils.TFidf(corpus)

def topN(url):
    content = db.test_search.find_one({"_id": url})
    if not content: return None
    topWords = TFidf.topN(content['content'], N=10)
    query = ' '.join(topWords)
    return query


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


def pretty_searchtext(text):
    if text is None:
        return text
    replace_list = ['\n', '\t', '-', ',', '|', '>', '<']
    for char in replace_list:
        text = text.replace(char, "")
    if text == "":
        return None
    text = text.split()
    return ' '.join(text)


def title_match(url, SE='google'):
    # TODO implement search on titleExact Match
    anchor = ""
    db = MongoClient().web_decay
    html = db.test_search.find_one({'_id': url})['html']
    html = brotli.decompress(html).decode()
    try:
        searchtext = text_utils.extract_title(html, version='newspaper')
    except:
        searchtext = get_headers(html)
    if searchtext is None or searchtext == 'Wayback Machine' or searchtext == "":
        return topN(url)
    searchtext = pretty_searchtext(searchtext)
    if SE == 'bing': anchor = "+"
    searchtext = "{}\"{}\"".format(anchor, searchtext)
    return searchtext


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


def trend_search():
    queries = open('queries.txt', 'r').read().split('\n')
    while queries[-1] == "": del queries[-1]
    for i, query in enumerate(queries):
        print(i, query)
        start = int(random.randint(50, 90) // 10 * 10)
        param_dict = {"start": start}
        searched_links = google_search(query, param_dict=param_dict)
        if len(searched_links) <= 0: continue
        url = random.choice(searched_links)
        try:
            r = requests.get(url, timeout=10, headers=requests_header)
            html = brotli.compress(r.text.encode())
            db.test_search.insert_one({
                "_id": url,
                "url": url,
                "html": html
            })
        except Exception as e:
            print(str(e))


def extract_content():
    filter_ext = ['.pdf']
    cursor = list(db.test_metadata_search.find({"content": {"$exists": False}}))
    for url in cursor:
        if os.path.splitext(url['url'])[1] in filter_ext: continue 
        if url.get('content'): continue
        html = brotli.decompress(url['html']).decode()
        print(url['url'])
        if html == '':
            db.test_metadata_search.delete_many({'url': url['url']})
            continue
        content = text_utils.extract_body(html)
        html = brotli.compress(html.encode())
        try:
            db.test_metadata_search.update_one({"_id": url['_id']}, {"$set": {"html": html, "content": content} })
        except Exception as e:
            print(str(e))

# extract_content()
# trend_search()


def metadata_search():
    for i, url in enumerate(list(db.test_search.find())):
        top, title = url['topN'], url['titleMatch']
        print(i, url['url'])
        if db.test_metadata_search.find_one({"from": url['url']}): continue
        top_urls = google_search(top)
        title_urls = google_search(title)
        top5 = set(top_urls[:5] + title_urls[:5])
        top10 = top_urls + title_urls
        obj = []
        for search_url in top10:
            try:
                print("\t" + search_url)
                r = requests.get(search_url, timeout=10, headers=requests_header)
                html = r.text
                obj.append({
                    "url": search_url,
                    "from": url['url'],
                    "html": brotli.compress(html.encode()),
                    "rank": "top5" if search_url in top5 else "top10"
                })
            except Exception as e:
                print(str(e))
        try:
            db.test_metadata_search.insert_many(obj, ordered=False)
        except Exception as e:
            print(str(e))


if __name__ == '__main__':
    extract_content()     

