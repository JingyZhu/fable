"""
Scripts dealing with links in html
    - Extract links
    - Categorizing links status code
"""
import requests
import os
import json
from urlextract import URLExtract
import threading
from urllib.parse import urlparse, urlunparse, urlsplit

BASEURL = 'html'

redir_data = { year: [] for year in range(2004, 2020) }
lock = threading.Lock()
count = 0


def fix_link(link):
    """
    Fix the handwritten links
    """
    parse_val = urlparse(link)
    if parse_val.path == '':
        parse_val._replace(path='/')
    parse_val._replace(netloc=parse_val.netloc.lower())
    return urlunparse(parse_val)


def is_home_redirect(link, new_link):
    # http to https
    old_parse, new_parse = urlparse(link), urlparse(new_link)
    if link.split('://')[1] == new_link.split('://')[1]:
        return True
    if new_parse.path == '/' or new_parse.path == '':
        if old_parse.netloc in new_parse.netloc or new_parse.netloc in old_parse.netloc:
            if old_parse.path != '/' and old_parse.path != '':
                return True
    return False


def link_categorize(year, links):
    global count
    for link in links:
        lock.acquire()
        print(count, link)
        count += 1
        lock.release()
        link = fix_link(link)
        try:
            r = requests.get(link, timeout=5)
        except Exception as e:
            print(str(e))
            continue
        if r.url != link:
            redir_link = r.url
            if is_home_redirect(link, redir_link):
                continue
            metadata = [link, redir_link]
            lock.acquire()
            redir_data[year].append(metadata)
            lock.release()

def url_complete(link):
    """
    Complete the link for: non-scheme, end slash
    """
    if "://" not in link:
        link = 'http://' + link
    parse_val = list(urlparse(link))
    # Add slash to path for homepage
    if parse_val[2] == "" or parse_val[2] == 'index.html':
        parse_val[2] = "/"
    return urlunparse(parse_val)


def extract_from_htmls():
    """
    Extracting links given all the crawled html
    Input: Dir of htmls
    Output: All the links in the htmls keyed by year
    """
    links = {}
    for i in range(2004, 2020):
        dir_list = os.listdir(os.path.join('html', str(i)))
        for html in dir_list:
            # print(i, html)
            text = open(os.path.join('html', str(i), html), 'r').read()
            extractor = URLExtract()
            try:
                link_list = list(set(extractor.find_urls(text)))
            except Exception as e:
                print("Exception: ", str(e))
                continue
            for link in link_list:
                link = url_complete(link)
                print(link)
                if link not in links:
                    links[link] = {}
                if i not in links[link]:
                    links[link][i] = []
                links[link][i].append(html)
    f = open('link_year.json', 'w+')
    json.dump(links, f)
    

def main():
    extract_from_htmls()
    # threads = []
    # file = open('links.json', 'r')
    # data = json.load(file)
    # print("Total links: ", sum([len(li) for li in data.values()]))
    # for k, v in data.items():
    #     threads.append(threading.Thread(target=link_categorize, args=(int(k), v[:100], )))
    #     threads[-1].start()
    # for i in range(len(data.keys())):
    #     threads[i].join()
    # file = open('redirect_links.json', 'w+')
    # json.dump(redir_data, file)



if __name__ == '__main__':
    main()