"""
Get all the links on https://seclists.org/interesting-people/index.html
"""
import requests
import os
from pyquery import PyQuery as pq
import queue

foot

BASE_URL = 'https://seclists.org/interesting-people/'
url_dict = {}
url_set = {}
posts_count = {}

class ThreadPool:
    def __init__(self, num_thread, event_handler):
        self.event_handler = event_handler
        self.queue = queue.Queue()
        for year in years:
            for month in months:
                self.queue.put([os.path.join(BASE_URL, str(year), month) + '/', year])
        self.num_thread = num_thread
        self.pool = [threading.Thread(target=self.worker_func, args=(i,) ) for i in range(self.num_thread)]
        for t in self.pool:
            t.start()
    
    def worker_func(self, i):
        while True:
            item = self.queue.get()
            try:
                self.event_handler(item, self)
            except Exception as e:
                print("Error for url {}:\n\t{}".format(item[0], str(e)))
            self.queue.task_done()

    
    def join(self):
        self.queue.join()

def parse_html(html):
    """
    Parse link (href) in the posts from a html
    return:
        absolute link
    """
    links = []
    dom = pq(html)
    pre = dom('pre')
    for p in pre.items():
        link_list = p('a')
        for a in link_list.items():
            link = a.attr['href']
            if link in url_set:
                print("occurred: {}".format(link))
            url_set[link] = 'dummy'
            links.append(link)
    return links


def event_handler(item, thread_pool):
    """
    Send request to url in item
    parse if response is html
    
    item: [url, year]
    """
    url = item[0]
    year = int(item[1])
    print("Trying to get: {}".format(url))
    try:
        req = requests.get(url)
    except Exception as e:
        print("Unable to request for url: {}\nError: {}".format(url, str(e) ))
        return url
    if req.status_code != 200:
        print("Uri: {} status code is {}".format(url, req.status_code))
        return url
    link_list = parse_html(req.text)


def dict_2_queue(posts_count):
    """
    Transfer dict to multithreading queue
    Input: dict[year][month]: #post
    Output: Q[[urls....., year]]
    """
    threadQ = queue.Queue()
    for y, v in posts_count.items():
        for m, n in v.items():
            for i in range(n):
                threadQ.put([os.path.join(BASE_URL, y, m, str(i)) + '/', int(y)])
    return threadQ
    

def main():
    r = requests.get('https://seclists.org/interesting-people/')
    dom = pq(r.text)
    table = dom(".calendar")
    links = table('a')
    for a in links.items():
        url = a.attr['href'].split('/')
        year, month = url[-3], url[-2]
        if year not in posts_count:
            posts_count[year] = {}
        posts_count[year][month] = int(a.text())
    threadQ = dict_2_queue(posts_count)
    req = requests.get('https://seclists.org/interesting-people/2004/Oct/10')
    print(parse_html(req.text))
    



if __name__ == '__main__':
    main()