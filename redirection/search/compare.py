"""
Extract content from htmls
Calculate the tf-idf scores for each pairs, and get the highest one
Output metadata into MongoDB
"""
import os
import json
from pymongo import MongoClient
import sys
import brotli

sys.path.append('../../')
from utils import text_utils

db = MongoClient().web_decay

def init_tfidf():
    url_docs = {}
    for obj in db.search_html.find():
        url = obj["url"]
        wayback_content = db.redirection.find_one({"url": url})['wayback_content']
        wayback_content = "" if wayback_content is None else wayback_content
        url_docs[url] = wayback_content
        topN = obj['topN'] if 'topN' in obj else []
        titlematch = obj['titlematch'] if 'titlematch' in obj else []
        for matchurls in topN:
            print(matchurls['search_url'])
            content = text_utils.extract_body(brotli.decompress(matchurls['html']).decode(), version='justext')
            if content is None:
                content = ""
            url_docs[matchurls['search_url']] = content
        for matchurls in titlematch:
            print(matchurls['search_url'])
            content = text_utils.extract_body(brotli.decompress(matchurls['html']).decode(), version='justext')
            if content is None:
                content = ""
            url_docs[matchurls['search_url']] = content
    return url_docs, text_utils.TFidf(list(url_docs.values()))


def comp2():
    """
    Calculate similarity of docs and get the highest one for each url
    """
    url_docs, TFidf = init_tfidf()
    for obj in db.search_html.find():
        url = obj["url"]
        newobj = {
            "url": url,
            "wayback_content": url_docs[url],
        }
        topN = obj["topN"] if 'topN' in obj else []
        best_url_topN, simi_topN = "", 0
        for matchurls in topN:
            matchurl = matchurls['search_url']
            # if matchurl not in url_docs:
            #     continue
            similarity = TFidf.similar(url_docs[url], url_docs[matchurl])
            if similarity >= simi_topN:
                simi_topN = similarity
                best_url_topN = matchurl
        if best_url_topN != "":
            newobj.update({
                "topN_url": best_url_topN,
                "topN_similarity": simi_topN, 
                "topN_content": url_docs[best_url_topN],
            })
        titlematch = obj["titlematch"] if 'titlematch' in obj else []
        best_url_titlematch, simi_titlematch = "", 0
        for matchurls in titlematch:
            matchurl = matchurls['search_url']
            similarity = TFidf.similar(url_docs[url], url_docs[matchurl])
            if similarity >= simi_titlematch:
                simi_titlematch = similarity
                best_url_titlematch = matchurl
        if best_url_titlematch != "":
            newobj.update({
                "titlematch_url": best_url_titlematch,
                "titlematch_similarity": simi_titlematch, 
                "titlematch_content": url_docs[best_url_titlematch],
            })
        db.search.insert_one(newobj)

comp2()