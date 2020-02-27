"""
Test the performance of query extraction from the HTML on the search engine
"""
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
import matplotlib.pyplot as plt

sys.path.append('../')
import config
from utils import text_utils, plot, search

db = MongoClient(config.MONGO_HOSTNAME).web_decay


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


def performance_trend():
    """
    Get the maximum similarity of the google search query
    Top5 and Top10
    """
    origin_corpus = list(db.test_search.find({"content": {"$exists": True}}, {"content": True}))
    origin_corpus = [e['content'] for e in origin_corpus]
    search_corpus = list(db.test_metadata_search.find({"content": {"$exists": True}}, {"content": True}))
    search_corpus = [e['content'] for e in search_corpus]
    TFidf = text_utils.TFidf(origin_corpus + search_corpus)
    top5, top10 = [], []
    for url in db.test_search.find({"content": {"$exists": True}}):
        content = url['content']
        print(url['url'])
        search_results = list(db.test_metadata_search.find({"from": url['url']}))
        t5 = [TFidf.similar(content, u['content']) for u in search_results if u['rank'] == 'top5']
        t10 = [TFidf.similar(content, u['content']) for u in search_results]
        t5 = max(t5) if len(t5) > 0 else 0
        t10 = max(t10) if len(t10) > 0 else 0
        top5.append(t5)
        top10.append(t10)
    plot.plot_CDF([top5, top10], classname=['top5', 'top10'], show=False)
    plt.xlabel("Pages")
    plt.ylabel("Similarity")
    plt.title("Search result similarities")


def calculate_titleMatch_topN():
    corpus1 = db.search_sanity_meta.find({}, {'content': True})
    corpus1 = [c['content'] for c in corpus1]
    corpus2 = db.search_meta.find({}, {'content': True})
    corpus2 = [c['content'] for c in corpus2]
    corpus = corpus1 + corpus2
    tfidf = text_utils.TFidf(corpus)
    print("TD-IDF initialized success!")
    urls = list(db.search_sanity_meta.find())
    for i, url in enumerate(urls):
        html = brotli.decompress(url['html']).decode()
        title = search.get_title(html)
        topN = tfidf.topN(url['content'])
        topN = ' '.join(topN)
        db.search_sanity_meta.update_one({'url': url['url']}, {'$set': {"topN": topN, "titleMatch": title}})
        if i % 100 == 0: print(i)


def search_titleMatch_topN():
    urls = db.search_sanity_meta.aggregate([
        {"$lookup":{
            "from": "searched_titleMatch",
            "localField": "url",
            "foreignField": "_id",
            "as": "hasSearched"
        }},
        {"$match": {"hasSearched.0": {"$exists": False}}},
        {"$project": {"hasSearched": False}}
    ])
    se_objs = []
    print('total:', len(urls))
    exit(0)
    for i, obj in enumerate(urls):
        titleMatch, topN, url = obj['titleMatch'], obj.get('topN'), obj['url']
        db.searched_titleMatch.insert_one({"_id": url})
        db.searched_topN.insert_one({"_id": url})
        if titleMatch:
            search_results = search.google_search('"{}"'.format(titleMatch))
            if search_results is None:
                print("No more access to google api")
                break
            print(i, len(search_results), url, titleMatch)
            for j, search_url in enumerate(search_results):
                se_objs.append({
                    "url": search_url,
                    "from": url,
                    "rank": "top5" if j < 5 else "top10"
                })
        if topN:
            search_results = search.google_search(topN)
            if search_results is None:
                print("No more access to google api")
                break
            print(i, len(search_results), url, topN)
            for j, search_url in enumerate(search_results):
                se_objs.append({
                    "url": search_url,
                    "from": url,
                    "rank": "top5" if j < 5 else "top10"
                })
        if len(se_objs) >= 10:
            try: db.search_sanity.insert_many(se_objs, ordered=False)
            except: pass
            se_objs = []

    try: db.search_sanity.insert_many(se_objs, ordered=False)
    except: pass
    


def performance_nonbroken():
    """
    Sanity check for search engine
    Search for copies for surely nonbroken pages
    Pipeline: calculate_titleMatch_topN --> search_titleMatch_topN --> compute_similarity
    """
    pass
    



if __name__ == '__main__':
    calculate_titleMatch_topN()
