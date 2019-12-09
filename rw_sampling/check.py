import json
from urllib.parse import urlparse
import threading
import queue
from matplotlib import pyplot as plt
import random
import os

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
    """
    Check the year of when wayback machien first crawl this domain given the filename
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
            if 'http' in hostname:
                hostname = urlparse(hostname).netloc
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

# data = json.load(open('hosts_year.json', 'r'))
# plot.plot_CDF([list(data.values())], classname=['check'])


def sample_diff_check(years):
    results = []
    for year in years:
        data = json.load(open('diff/diff_{}.json'.format(year), 'r'))
        new_urls = data['gain']
        sample = random.sample(new_urls, 1000)
        sample = {k: "" for k in sample}
        json.dump(sample, open('temp.json', 'w+'))
        check_wayback_contains('temp.json', 1)
        os.remove('temp.json')
        result = json.load(open('hosts_year.json', 'r'))
        result = {k: int(v) for k, v in result.items()}
        results.append(result.values())
    plot.plot_CDF(results, classname=years)

# differences = [[2634994, 456636, 1119435], 
#         [2248809, 232753, 620360], 
#         [1673987, 3466160, 2579478]
# ]
# years = ['2011', '2013', '2017']
# plot.plot_bargroup(differences, years, ['lost', 'gain', 'shared'])

sample_diff_check(['2011', '2013', '2017'])
