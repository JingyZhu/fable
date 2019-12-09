import json
from urllib.parse import urlparse
import threading
import queue

import sys
sys.path.append('../')
from utils import crawl, plot


def hosts_in():
    """
    Check how many of the hosts are in DMOZ urls
    """
    url_db = json.load(open('url_db_2017.json', 'r'))
    hosts = json.load(open('hosts.json', 'r'))
    urls = set() 
    for obj in url_db:
        urls.add(urlparse(obj['url']).netloc) 

    count = 0
    for k in hosts:
        for w in urls:
            if k in w:
                print(k, w)
                count += 1
                break

    print(count, len(hosts))
    # 5084 10104 for 10k
# 139 1016

def check_wayback_contains(filename, NUM_THREADS=10):
    # TODO implement this function
    """
    filename is a KV pair with key as the hostname
    """
    data = json.load(open(filename, 'r'))
    q_in, q_out = queue.Queue(), queue.Queue()
    for k in data:
        q_in.put(k)
    def thread_func(q_in, q_out):
        param_dict = {
            'collapse': 'timestamp:4',
            'limit': 10
        }
        while not q_in.empty():
            hostname = q_in.get()
            print(hostname)
            url_list, rval = crawl.wayback_index('*.{}/*'.format(hostname), param_dict=param_dict)
            if rval != "Success":
                print(rval)
                q_out.put((hostname, '2020'))
            else:
                q_out.put((hostname, str(url_list[0][0])[:4] ))
    t_pools = []
    for _ in range(NUM_THREADS):
        t_pools.append(threading.Thread(target=thread_func, args=(q_in, q_out)))
        t_pools[-1].start()
    for i in range(NUM_THREADS):
        t_pools[i].join()
    host_year = {}
    while not q_out.empty():
        hostname, year = q_out.get()
        host_year[hostname] = year
    json.dump(host_year, open('hosts_year.json', 'w+'))


# check_wayback_contains('hosts_10k.json', NUM_THREADS=1)

data = json.load(open('hosts_year.json', 'r'))
plot.plot_CDF([list(data.values())], classname=['check'])