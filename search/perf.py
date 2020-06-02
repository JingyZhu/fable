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
import multiprocessing as mp
import pymongo
import collections
from urllib.parse import urlparse

sys.path.append('../')
import config
from utils import text_utils, plot, search, crawl, url_utils

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
counter = mp.Value('i', 0)
host_extractor = url_utils.HostExtractor()

def topN(url):
    content = db.test_search.find_one({"_id": url})
    if not content: return None
    topWords = TFidfDynamic.topN(content['content'], N=10)
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
    TFidfDynamic = text_utils.TFidfDynamic(origin_corpus + search_corpus)
    top5, top10 = [], []
    for url in db.test_search.find({"content": {"$exists": True}}):
        content = url['content']
        print(url['url'])
        search_results = list(db.test_metadata_search.find({"from": url['url']}))
        t5 = [TFidfDynamic.similar(content, u['content']) for u in search_results if u['rank'] == 'top5']
        t10 = [TFidfDynamic.similar(content, u['content']) for u in search_results]
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
    tfidf = text_utils.TFidfDynamic(corpus)
    print("TD-IDF initialized success!")
    urls = list(db.search_sanity_meta.find())
    for i, url in enumerate(urls):
        html = brotli.decompress(url['html']).decode()
        title = search.get_title(html)
        topN = tfidf.topN(url['content'])
        topN = ' '.join(topN)
        db.search_sanity_meta.update_one({'url': url['url']}, {'$set': {"topN": topN, "titleMatch": title}})
        if i % 100 == 0: print(i)


def crawl_realweb(q_in, tid):
    global counter
    se_ops = []
    db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
    while not q_in.empty():
        url, fromm, idd = q_in.get()
        with counter.get_lock():
            counter.value += 1
            print(counter.value, tid, url)
        html = crawl.requests_crawl(url)
        if html is None: html = ''
        content = text_utils.extract_body(html, version='domdistiller')
        se_ops.append(pymongo.UpdateOne(
            {"_id": idd}, 
            {"$set": {"html": brotli.compress(html.encode()), "content": content}}
        ))
        if len(se_ops) >= 1:
            try: db.search_sanity.bulk_write(se_ops, ordered=False)
            except: print("db bulk write failed")
            se_ops = []
    try: db.search_sanity.bulk_write(se_ops)
    except: print("db bulk write failed")


def crawl_realweb_wrapper(NUM_THREADS=10):
    """
    Crawl the searched results from the db.search_sanity
    Update each record with html (byte) and content
    """
    q_in = mp.Queue()
    urls = db.search_sanity.find({'html': {"$exists": False}})
    urls = list(urls)
    random.shuffle(urls)
    print(len(urls))
    for url in urls:
        q_in.put((url['url'], url['from'], url["_id"]))
    pools = []
    for i in range(NUM_THREADS):
        pools.append(mp.Process(target=crawl_realweb, args=(q_in, i)))
        pools[-1].start()
    for t in pools:
        t.join()


def search_titleMatch_topN_google():
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
    urls = list(urls)
    print('total:', len(urls))
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


def search_titleMatch_topN_bing():
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
    urls = list(urls)
    print('total:', len(urls))
    for i, obj in enumerate(urls):
        titleMatch, topN, url = obj['titleMatch'], obj.get('topN'), obj['url']
        db.searched_titleMatch.insert_one({"_id": url})
        db.searched_topN.insert_one({"_id": url})
        try:
            today_url = requests.get(url).url
            sitename = host_extractor.extract(today_url)
        except: sitename = None
        if titleMatch:
            query = '+"{}"'.format(titleMatch)
            if sitename: query += " site:{}".format(sitename)
            search_results = search.bing_search(query)
            if search_results is None:
                print("No more access to search api")
                break
            print(i, len(search_results), url, query)
            for j, search_url in enumerate(search_results):
                se_objs.append({
                    "url": search_url,
                    "from": url,
                    "rank": "top5" if j < 5 else "top10"
                })
        if topN:
            if sitename: topN += " site:{}".format(sitename)
            search_results = search.bing_search(topN)
            if search_results is None:
                print("No more access to bing api")
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
    

def calculate_similarity():
    """
    Calcuate the (highest) similarity of each searched pages
    Update similarity and searched_urls to db.search_meta
    """
    corpus1 = db.search_sanity_meta.find({'content': {"$ne": ""}}, {'content': True})
    corpus2 = db.search_sanity.find({'content': {"$exists": True,"$ne": ""}}, {'content': True})
    corpus = [c['content'] for c in corpus1] + [c['content'] for c in corpus2]
    tfidf = text_utils.TFidfDynamic(corpus)
    print("tfidf init success!")
    searched_urls = db.search_sanity_meta.aggregate([
        {"$match": {"similarity": {"$exists": False}}},
        {"$lookup": {
            "from": "search_sanity",
            "localField": "_id",
            "foreignField": "from",
            "as": "searched"
        }},
        {"$project": {"searched.html": False, "searched._id": False, "_id": False, "html": False}},
        {"$unwind": "$searched"},
        {"$match": {"searched.content": {"$exists": True, "$ne": ""}}}
    ])
    searched_urls = list(searched_urls)
    simi_dict = collections.defaultdict(lambda: { 'simi': 0, 'searched_url': ''})
    print('total comparison:', len(searched_urls))
    for i, searched_url in enumerate(searched_urls):
        if i % 100 == 0: print(i)
        url, content = searched_url['url'], searched_url['content']
        searched = searched_url['searched']
        simi = tfidf.similar(content, searched['content'])
        if simi >= simi_dict[url]['simi']:
            simi_dict[url]['simi'] = simi
            simi_dict[url]['searched_url'] = searched['url']
    search_meta = db.search_sanity_meta.find({ "similarity": {"$exists": False}}, {'url': True})
    for obj in list(search_meta):
        url = obj['url']
        value = simi_dict[url]    
        db.search_sanity_meta.update_one({'_id': url}, \
            {'$set': {'similarity': value['simi'], 'searched_url': value['searched_url']}})


def performance_nonbroken():
    """
    Sanity check for search engine
    Search for copies for surely nonbroken pages
    Pipeline: calculate_titleMatch_topN --> search_titleMatch_topN --> compute_similarity
    """
    pass


def se_level_of_indexing():
    """
    For urls without indexing, find whether there exists other urls at the same dir
    """
    not_indexed_urls = db.search_sanity_prefix.find({'google_dir': {"$exists": False}})
    not_indexed_urls = list(not_indexed_urls)
    print("Total: ", len(not_indexed_urls))
    for i, url in enumerate(not_indexed_urls):
        print(i, url['url'])
        try:
            today_url = requests.get(url['url'], timeout=15).url
        except: today_url = url['url']
        up, today_up = urlparse(url['url']), urlparse(today_url)
        netloc, path, query = today_up.netloc, up.path, up.query
        dirname = path
        if path in ['', '/']:
            continue
        for _ in range(3):
            if dirname == '/':
                break
            dirname = os.path.dirname(dirname)
            site = netloc + dirname
            print('Search on google:', dirname)
            google_urls = search.google_search("", param_dict={'siteSearch': site})
            if len(google_urls) > 0:
                db.search_sanity_prefix.update_one({"_id": url['_id']}, {"$set": {"google_dir": dirname, 'google_urls': google_urls}})
                break
        dirname = path
        for _ in range(3):
            if dirname == '/':
                break
            dirname = os.path.dirname(dirname)
            site = netloc + dirname
            print('Search on bing:', dirname)
            bing_urls = search.bing_search("site:{}".format(site))
            if len(bing_urls) > 0:
                db.search_sanity_prefix.update_one({"_id": url['_id']}, {"$set": {"bing_dir": dirname, 'bing_urls': bing_urls}})
                break


if __name__ == '__main__':
    se_level_of_indexing()
