"""
Compare the tf-idf similarity of two documents (extract content)
Plot the similarity as CDF
"""
import json
from utils import text_utils
from utils import plot
import matplotlib.pyplot as plt
from urllib.parse import urlparse
import os
from multiprocessing import Queue, Process, Value, Pool
from pymongo import MongoClient


similarity = []
detailed = {}

# blacklist and jcontent extraction version
blacklist = ['bloomberg.com', 'bloombergview.com', 'businessweek.com']
blackextension = ['.pdf']
VERSION = 'justext'

# Parallel vars
input_queue = []
output_queue = Queue(maxsize=500)
counter = Value('i', 0)
NUM_Process = 1


wayback_htmls = json.load(open('wayback/wayback_html_sample.json', 'r'))
origin_htmls = json.load(open('origin/origin_html_sample.json', 'r'))
url_data = json.load(open('wayback/wayback_urls_sample.json', 'r'))
db = MongoClient().web_decay


def filter_blacklist(url_dict):
    """
    Delete the keys which are in the blacklist
    """
    new_dict = {}
    for key, value in url_dict.items():
        found = False
        for black_url in blacklist:
            if black_url in urlparse(key).netloc:
                found = True
                break
        for ext in blackextension:
            # Vprint(os.path.splitext(urlparse(key).path)[1])
            if ext == os.path.splitext(urlparse(key).path)[1]:
                found = True
                break
        if found == True:
            continue
        new_dict[key] = value
    return new_dict


def comp1_parallel(rval):
    # TODO Modify to adapt new tfidf calculation
    url, htmls = rval
    counter.acquire()
    print(counter.value, url)
    counter.value += 1
    counter.release()
    if len(htmls) == 0 or url not in origin_htmls or origin_htmls[url] is None:
        return
    origin_text = text_utils.extract_body(origin_htmls[url], VERSION)
    ts_simi, wayback_texts = {}, {}
    for timestamp, html in htmls.items():
        wayback_texts[timestamp] = text_utils.extract_body(html, VERSION)
        ts_simi[timestamp] = text_utils.similar(wayback_texts[timestamp], origin_text)
    max_key = max(ts_simi, key=lambda x: ts_simi[x]) # Get key with max value
    similarity = ts_simi[max_key]
    return (url, {"wayback_url": url_data[url]['wayback_url'], "similarity": similarity, "timestamp": max_key}, {"origin_content": origin_text, "wayback_content": wayback_texts[max_key]})


def comp1():
    """
    Do the comparison 1: (current html vs. wayback html)
    Compare the original html with the three wayback html, and pick the highest matching one
    """
    wayback_htmls = json.load(open('wayback/wayback_html_sample.json', 'r'))
    origin_htmls = json.load(open('origin/origin_html_sample.json', 'r'))
    url_data = json.load(open('wayback/wayback_urls_sample.json', 'r'))

    wayback_htmls = filter_blacklist(wayback_htmls)
    for i, (url, htmls) in enumerate(wayback_htmls.items()):
        print(i, url)
        if len(htmls) == 0 or url not in origin_htmls or origin_htmls[url] is None:
            continue
        origin_text = text_utils.extract_body(origin_htmls[url], VERSION)
        ts_simi = {}
        for timestamp, html in htmls.items():
            wayback_text = text_utils.extract_body(html, VERSION)
            ts_simi[timestamp] = text_utils.similar(wayback_text, origin_text)
        max_key = max(ts_simi, key=lambda x: ts_simi[x]) # Get key with max value
        similarity.append(ts_simi[max_key])
        # print(len(htmls), ts_simi[max_key])
        detailed[url] = {"wayback_url": url_data[url]['wayback_url'], "similarity": similarity[-1], "timestamp": max_key} # , "origin_text": origin_text, "wayback_text": wayback_text}
    
    print("Total html comparison:", len(similarity))
    plot.cdf_plot([similarity])
    plt.xlabel("TF-IDF similarity")
    plt.ylabel("CDF")
    plt.title(VERSION)
    plt.show()
    json.dump(detailed, open('data/comp1.json','w+') )



def comp1_db():
    """
    Do comparison from data in db
    Get url, content from redirection, get tf-idf from class, and update in the redirection
    """
    url_idx, TFidfDynamic = text_utils.prepare_tfidf()
    db = MongoClient().web_decay
    redirection = db.redirection
    for i, obj in enumerate(redirection.find()):
        url = obj['url']
        if not obj['original_content'] or not obj['wayback_content']:
            similarity = 0
        else:
            similarity = TFidfDynamic.similar(obj['original_content'], obj['wayback_content'])
        redirection.find_one_and_update(
            {'url': url}, {'$set': {'similarity': similarity}}
        )
    




def main():
    comp1_db()


if __name__ == '__main__':
    main()
    # wayback_htmls = filter_blacklist(wayback_htmls)
    # for url, htmls in wayback_htmls.items():
    #     input_queue.append((url, htmls))

    # pool = Pool()
    # output_queue = pool.map(comp1_parallel, input_queue)
    # output_queue = list(filter(lambda x: x is not None, output_queue))
    
    # detailed = {v[0]: v[1] for v in output_queue}

    # similarity = [v['similarity'] for v in detailed.values()]
    # plot.cdf_plot([similarity])
    # plt.xlabel("TF-IDF similarity")
    # plt.ylabel("CDF")
    # plt.title(VERSION)
    # plt.show()
    # json.dump(detailed, open('data/comp1.json','w+') )

    # Dump content json
    # original_contents = {v[0]: v[2]['origin_content'] for v in output_queue}
    # wayback_contents = {v[0]: v[2]['wayback_content'] for v in output_queue}
    # json.dump(original_contents, open('data/origin_content.json','w+') )
    # json.dump(wayback_contents, open('data/wayback_content.json','w+') )