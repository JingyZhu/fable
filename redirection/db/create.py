"""
Create and put data into MongoDB
web_decay 
--> redirection
    url
    to
    year
    wayback_url
    match
    original_content
    wayback_content
--> html
    url
    origin_html
    wayback_html
"""
import pymongo
from pymongo import MongoClient
import json
import pprint
import brotli

client = MongoClient()
db = client.web_decay

manual_sample = json.load(open('../wayback/wayback_urls_sample.json', 'r'))
sample_500 = json.load(open('../wayback/wayback_urls_sample.json', 'r'))
original_content = json.load(open('../data/origin_content.json', 'r'))
wayback_content = json.load(open('../data/wayback_content.json', 'r'))

def create_redirection():
    redirection = db.redirection

    for url, value in manual_sample.items():
        if url not in original_content or url not in wayback_content:
            continue
        obj = {
            "url": url, 
            "to": sample_500[url]["To"],
            "year": sample_500[url]["year"],
            "wayback_url": value["wayback_url"],
            "original_content": original_content[url],
            "wayback_content": wayback_content[url]
        }
        redirection.insert_one(obj)


def creaate_html():
    original_html = json.load(open('../origin/origin_html_sample.json', 'r'))
    wayback_html = json.load(open('../wayback/wayback_html_sample.json', 'r'))

    html = db.html
    for url, value in wayback_html.items():
        obj = {
            "url": url, 
            "origin_html": original_html[url],
            "wayback_html": wayback_html[url]
        }
        html.insert_one(obj)


def create_search_html():
    """
    Create search html with brotli compression
    """
    htmls_topN = json.load(open('../search/search_html_topN.json', 'r'))
    htmls_titlematch = json.load(open('../search/search_html_titlematch.json', 'r'))
    for url, value in htmls_topN.items():
        obj = {
            "url": url,
            "topN": [
                {
                    "search_url": su,
                    "html": brotli.compress(h.encode())
                }
            for su, h in value.items()]
            }
        if url in htmls_titlematch:
            obj.update({
                'titlematch': [{
                    "search_url": su,
                    "html": brotli.compress(h.encode())
                } for su, h in htmls_titlematch[url].items()]
            })
        db.search_html.insert_one(obj)
    
    for url, value in htmls_titlematch.items():
        if url in htmls_topN:
            continue
        obj = {
            "url": url,
            "titlematch": [
                {
                    "search_url": su,
                    "html": brotli.compress(h.encode())
                }
            for su, h in value.items()]
            }
        db.search_html.insert_one(obj)



def update_search_html_tocontent():
    htmls = json.load(open('../search/search_html_titlematch.json', 'r'))
    for url, value in htmls.items():
        titlematch= [
                {
                    "search_url": su,
                    "html": h
                }
            for su, h in value.items()]
        db.search_html.find_one_and_update({"url": url}, {"$set": {"titlematch": titlematch}})


create_search_html()