import json


url = 'http://www.pcinpact.com/actu/news/50209-liberation-deneuvre-april-hadopi-critiques.htm'
OUTPUT_FILE = 'test.csv'

from bs4 import BeautifulSoup, Comment
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import csv
import dragnet
import justext

version = 'justext'

def beautify(text):
    replace_char = ["\n", "\t"]
    for c in replace_char:
        text = text.replace(c, "")
    return text


def extract_text(html):
    soup = BeautifulSoup(html, "html.parser")
    filter_tags = ['a', 'style', 'script', 'textarea']
    for tag in filter_tags:
        for element in soup.findAll(tag):
            element.decompose()
    text = ""
    for t in soup.find_all(text=True):
        if not isinstance(t, Comment) and len(t.split(" ")) > 5:
            text += " " + t
    return beautify(text)



def justext_extract(html):
    main_text = ""
    paragraphs = justext.justext(html, justext.get_stoplist('English'))
    for paragraph in paragraphs:
        if not paragraph.is_boilerplate:
            main_text += paragraph.text
    return main_text


def extract_body(html, version='mine'):
    lib_dict = { 
        'mine': extract_text, 
        'dragnet': dragnet.extract_content,
        'justext': justext_extract
    }
    return lib_dict[version](html)


def get_html():
    wayback_url = json.load(open('../wayback/wayback_html_sample.json', 'r'))
    origin_url = json.load(open('../origin/origin_html_sample.json', 'r'))
    timestamp_data = json.load(open('../data/comp1.json', 'r'))

    timestamp = timestamp_data[url]['timestamp']
    f = open('wayback.html', 'w+')
    f.write(wayback_url[url][timestamp])
    f = open('origin.html', 'w+')
    f.write(origin_url[url])
    return [url, timestamp_data[url]['wayback_url']]


def extract():
    wayback_html = open('wayback.html', 'r').read()
    origin_html = open('origin.html', 'r').read()
    return [extract_body(origin_html, version), extract_body(wayback_html, version)]


def store_csv(text_list):
    csv_writer = csv.writer(open(OUTPUT_FILE, 'w+'))
    csv_writer.writerows(text_list)


# import json
# data = json.load(open('wayback_resp.json', 'r'))
# del data[0]
# a = {d[2]: 0 for d in data}
# print(len(a))
# json.dump(list(a.keys()), open('test.json', 'w+'))

# get_html()