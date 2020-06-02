"""
Search urls by descriptions on the blog
"""
import sys
import requests
import os
from os.path import join
import json
from subprocess import call
from pymongo import MongoClient
from bs4 import BeautifulSoup

# sys.path.insert(1, '../')
# from utils import text_utils


url_match = {}

google_query_dict = {
    "q": None,
    "key" : "AIzaSyCq145QuNtIRGsluXo4n6lUbrvdOLA_hCY",
    "cx" : "006035121867626378213:tutaxpqyro8",
}

headers = {"Ocp-Apim-Subscription-Key": '978290f3b37c48538596753b4d2be65f'}

bing_query_dict = {
    "q": None
}

google_url = 'https://www.googleapis.com/customsearch/v1'
bing_url = 'https://api.cognitive.microsoft.com/bing/v7.0/search'


def topN(url):
    _, TFidfDynamic = text_utils.prepare_tfidf()
    db = MongoClient().web_decay
    content = db.redirection.find_one({'url': url})['wayback_content']
    if content is None:
        return None
    topWords = TFidfDynamic.topN(content, N=10)
    query = ' '.join(topWords)
    return query


def get_headers(html):
    soup = BeautifulSoup(html)
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


def title_match(url):
    # TODO implement search on titleExact Match
    _, TFidfDynamic = text_utils.prepare_tfidf()
    db = MongoClient().web_decay
    timestamp = db.redirection.find_one({'url': url}, {"timestamp": 1})['timestamp']
    element = db.html.find_one({'url': url}, {"wayback_html": 1})['wayback_html']
    html = list(filter(lambda x: x['timestamp'] == timestamp, element))[0]['html']
    try:
        searchtext = text_utils.extract_title(html, version='newspaper')
    except:
        searchtext = get_headers(html)
    if searchtext is None or searchtext == 'Wayback Machine' or searchtext == "":
        return topN(url)
    searchtext = pretty_searchtext(searchtext)
    searchtext = "+\"{}\"".format(searchtext)
    return searchtext


def google_search(query):
    """
    Search using google
    """
    google_query_dict["q"] = query
    try:
        r = requests.get(google_url, params=google_query_dict)
        r = r.json()
    except Exception as e:
        print(str(e))
        return []
    if "items" not in r:
        return []
    end = 5 if len(r["items"]) > 5 else len(r["items"])
    return [ u["link"] for u in r['items'][:end]]


def bing_search(query):
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
    end = 5 if len(values) > 5 else len(values)
    return [u['url'] for u in values[:end]]


def search_urls(urls):
    """
    Entry function for search similar webpages
    Use {topN, headers} * {google, bing} as keyword extractions and search engine
    Input: list of urls. wayback_html INCLUDED in MONGODB!
    Output: Dump results (has some searched url) to JSON.
    """
    get_query = topN
    search_func = bing_search
    for url in urls:
        query = get_query(url)
        print(url, query)
        if query is None:
            continue
        urls = search_func(query)
        if urls == []:
            continue
        url_match[url] = urls
    json.dump(url_match, open('search_match_topN.json', 'w+'))


def load_pages(search_dict, output_file='search_html_titlematch.json'):
    search_html = {}
    if os.path.exists(output_file):
        search_html = json.load(open(output_file, 'r'))
    count = 0
    for key, urls in search_dict.items():
        search_html.setdefault(key, {})
        for url in urls:
            print(count, url)
            count += 1
            if url in search_html:
                continue
            try:
                call(['node', '../run.js', url], timeout=120)
            except Exception as e:
                print(str(e))
                call(['pkill', 'chrome'])
                continue
            html = open('temp.html', 'r').read()
            search_html[key][url] = html

            if os.path.exists('temp.html'):
                os.remove("temp.html")
            
            if count % 5 == 0:
                json.dump(search_html, open(output_file, 'w+'))
    
    json.dump(search_html, open(output_file, 'w+'))


if __name__ == '__main__':
    # missing_urls = json.load(open('missing_urls.json', 'r'))
    # search_urls(missing_urls)
    search_dict = json.load(open('search_match_titlematch.json', 'r'))
    load_pages(search_dict, 'search_html_titlematch.json')
    search_dict = json.load(open('search_match_topN.json', 'r'))
    load_pages(search_dict, 'search_html_topN.json')
    