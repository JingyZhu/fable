"""
Check whether a page is broken
"""
import requests
import os, shutil
from urllib.parse import urlparse 
from pymongo import MongoClient
import pymongo
import sys
import threading, queue
import json
import socket
import random
import re
import subprocess
import nmap
# from collections import defaultdict

sys.path.append('../')
import config
from utils import db_utils, url_utils, sic_transit

db = MongoClient(config.MONGO_HOSTNAME, username=config.MONGO_USER, password=config.MONGO_PWD, authSource='admin').web_decay
year = 2014
NUM_THREADS = 10
counter = 0

netloc_othererror = {}

def send_request(url):
    '''
    Send one request to url.
    The documentation of various types of request errors is found here.

    https://2.python-requests.org/en/master/api/#exceptions
    '''

    resp = None
    requests_header = {'user-agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"}

    # TODO: look into the fail reason in ConnectionError and find which other ones are related to DNS.
    req_failed = True
    try:
        resp = requests.get(url, headers=requests_header, timeout=15)
        req_failed = False
    # Requsts timeout
    except requests.exceptions.ReadTimeout:
        error_msg = 'ReadTimeout'
    except requests.exceptions.Timeout:
        error_msg = 'Timeout'
    # DNS Error or tls certificate verify failed
    except requests.exceptions.ConnectionError as exc:
        reason = str(exc)
        # after looking for the failure info, the following should be the errno for DNS errors.
        if ("[Errno 11001] getaddrinfo failed" in reason or     # Windows
            "[Errno -2] Name or service not known" in reason or # Linux
            "[Errno 8] nodename nor servname " in reason):      # OS X
            error_msg = 'ConnectionError_DNSLookupError'
        else:
            error_msg = 'ConnectionError'
    except requests.exceptions.MissingSchema:
        error_msg = 'MissingSchema'
    except requests.exceptions.InvalidSchema:
        error_msg = 'InvalidSchema'
    except requests.exceptions.RequestException:
        error_msg = 'RequestException'
    except requests.exceptions.TooManyRedirects:
        error_msg = 'TooManyRedirects'
    except UnicodeError:
        error_msg = 'ERROR_UNICODE'
    except Exception as _:
        error_msg = 'ERROR_REQUEST_EXCEPTION_OCCURRED'

    if req_failed:
        return resp, error_msg

    return resp, 'SUCCESSFUL'


def other_error(url):
    """
    Categorize other error by:
    DNS resolution
    Ping the IP
    nmap scan on {80, 443}
    Return on failure on which part. If all success, return listening port
    """
    netloc = urlparse(url).netloc.split(":")[0]
    if netloc in netloc_othererror: return netloc_othererror[netloc]
    try:
        ip = socket.gethostbyname(netloc)
    except:
        netloc_othererror[netloc] = "DNS lookup failure"
        return "DNS lookup failure"
    try:
        output = subprocess.check_output(['ping', '-c', '3', ip])
        result = re.compile('[0-9]+ received').findall(output.decode())[0]
        num_recv = int(result.split(' ')[0])
        if num_recv == 0: 
            netloc_othererror[netloc] = "Ping failure"
            return "Ping failure"
    except:
        netloc_othererror[netloc] = "Ping failure"
        return "Ping failure"
    nm = nmap.PortScanner()
    nm.scan(ip, arguments="-sT -Pn -p80,443")
    scan_dict = nm[ip]['tcp']
    r_str = '80_{}_443_{}'.format(scan_dict[80]['state'], scan_dict[443]['state'])
    netloc_othererror[netloc] = r_str
    return r_str


def get_status(url, resp, msg):
    status, detail = "", ""
    if msg == 'SUCCESSFUL':
        final_url, status_code = resp.url, resp.status_code
        url_path = urlparse(url).path
        final_url_path = urlparse(final_url).path
        # remove the last '/' if it exists
        if url_path.endswith('/'):
            url_path = url_path[:-1]
        if final_url_path.endswith('/'):
            final_url_path = final_url_path[:-1]
        
        status = str(status_code)
        # if the response status code is 400 or 500 level, brokem
        if int(status_code / 100) >= 4:
            detail = status_code
        # if status code is 200 level and no redirection
        elif (int(status_code/100) == 2 or int(status_code/100) == 3) and final_url_path == url_path:
            detail = 'no redirection'
        # if a non-hompage redirects to a homepage, considered broken
        elif final_url_path == '' and url_path != '':
            detail = 'homepage redirection'
        # if it redirects to another path, we are unsure.
        elif final_url_path != url_path:
            detail = 'non-home redirection'

        # do not know what redirection happens
        else:
            # this list should be empty
            detail = 'unknown redirection'
    else:
        if 'ConnectionError_DNSLookupError' in msg:
            status = 'DNSError'
        elif msg == 'TooManyRedirects':
            status = 'OtherError'
            detail = 'TooManyRedirects'
        else:
            status = 'OtherError'
            detail = other_error(url)
            if "DNS" in detail: status = "DNSError"
    return status, detail


def test_links(q_in):
    '''Send requests to each link and log their status.'''
    global counter
    objs, errors = [], []
    while not q_in.empty():
        url, hostname = q_in.get()
        counter += 1
        print(counter, url)
        resp, msg = send_request(url)
        status, detail = get_status(url, resp, msg)
        # .Temp added
        db.url_status.update_one({"_id": url}, {"$set": {"status": status, "detail": detail}})
        # End
    # Temp Commmented
    #     obj = {
    #         "_id": url,
    #         "url": url,
    #         "hostname": hostname,
    #         "year": year,
    #         "status": status,
    #         "detail": detail
    #     }
    #     objs.append(obj)
    #     if len(objs) >= 100:
    #         try:
    #             db.url_status.insert_many(objs, ordered=False)
    #         except Exception as e:
    #             print(str(e))
    #         objs = []
    # try:
    #     db.url_status.insert_many(objs, ordered=False)
    # except:
    #     pass
    # json.dump(errors, open("errors/errors_{}.json".format(socket.gethostname()), 'w+'))
    # End 
    

def sample_urls():
    """
    Sample urls from all hosts with added links >= N in year
    Put the sampled record into db.url_sample
    """
    db.url_sample.create_index([("hostname", pymongo.ASCENDING), ("year", pymongo.ASCENDING)])
    cursor = db_utils.Hosts_gte_N_links_in_year(db, 500, year)
    valid_hosts = [u['hostname'] for u in cursor]
    valid_hosts = set(filter(lambda x: '\n' not in x, valid_hosts))
    print(len(valid_hosts))
    for hostname in valid_hosts:
        urls_list = list(db.url_population.aggregate([{'$match': {'hostname': hostname, 'year': year}}, {'$sample': {'size': 100}}]))
        for url_obj in urls_list:
            url_obj['_id'] = url_obj['url']
        try:
            db.url_sample.insert_many(urls_list, ordered=False)
        except:
            db.url_sample.delete_many({'hostname': hostname})


def collect_status():
    """
    Collect status of urls
    """
    db.url_status.create_index([("hostname", pymongo.ASCENDING), ("year", pymongo.ASCENDING)])
    q_in = queue.Queue()
    hostnames = list(db.url_sample.aggregate([{"$match": {"year": year}}, {'$group': {"_id": "$hostname"}}]))
    hostnames = sorted([obj['_id'] for obj in hostnames])
    idx, length = config.HOSTS.index(socket.gethostname()), len(hostnames)
    host_shards = hostnames[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    temp = []
    for hostname in host_shards:
        # Temp Commented
        # if db.url_status.find_one({"hostname": hostname, "year": year}): continue
        # urls_list = db.url_sample.find({'hostname': hostname, 'year': year})
        # End
        # Temp added
        urls_list = db.url_status.find({'hostname': hostname, 'year': year, 'status': re.compile("^403")})
        # End
        for obj in urls_list:
            temp.append((obj['url'], hostname))
    random.shuffle(temp)
    for t in temp: q_in.put(t)
    pools = []
    for _ in range(NUM_THREADS):
        pools.append(threading.Thread(target=test_links, args=(q_in,)))
        pools[-1].start()
    for t in pools:
        t.join()


def test_45xx(q_in):
    global counter
    url_ops, search_ops = [], []
    while not q_in.empty():
        url, old_status = q_in.get()
        counter += 1
        print(counter, url)
        resp, msg = send_request(url)
        status, detail = get_status(url, resp, msg)
        if status != old_status:
            url_ops.append(pymongo.UpdateOne(
                {"_id": url}, 
                {"$set": {"status": status, "detail": detail}}
            ))
            if url_utils.status_categories(status, detail) != '4/5xx':  
                search_ops.append(pymongo.DeleteMany({"url": url}))
        if len(url_ops) >= 30:
            try: db.url_status_implicit_broken.bulk_write(url_ops)
            except: pass
            try: db.url_broken.bulk_write(url_ops)
            except: pass
            try: db.search_meta.bulk_write(search_ops)
            except: pass
            try: db.url_broken.bulk_write(search_ops)
            except: pass
            url_ops, search_ops = [], []
    try: db.url_status_implicit_broken.bulk_write(url_ops)
    except: pass
    try: db.url_broken.bulk_write(url_ops)
    except: pass
    try: db.search_meta.bulk_write(search_ops)
    except: pass
    try: db.url_broken.bulk_write(search_ops)
    except: pass


def fix_45xx_status(NUM_THREADS=10):
    """
    For 45xx urls, it could be temporal 45xx and considered as robot that causing the error
    Using new header and doing the crawl for 3 times. Consider 45xx only if all 3 times are the same
    Otherwise, update url_status_implicit_broken, url_broken, and delete search_meta
    """
    q_in = queue.Queue()
    urls = list(db.url_status_implicit_broken.find({'status': re.compile("^[45]")}))
    random.shuffle(urls)
    urls = urls + urls + urls
    for url in urls: q_in.put((url['url'], url['status']))
    print('total requests:', len(urls))
    pools = []
    for _ in range(NUM_THREADS):
        pools.append(threading.Thread(target=test_45xx, args=(q_in,)))
        pools[-1].start()
    for t in pools:
        t.join()


def other_error_update():
    def other_error_thread(q_in):
        global counter
        while not q_in.empty():
            counter += 1
            url = q_in.get()
            print(counter, url)
            error_code = other_error(url)
            db.url_status.update_one({"_id": url}, {"$set": {"error_code": error_code}})
    urls = db.url_status.find({"year": year, "status": "OtherError"})
    q_in = queue.Queue()
    urls = sorted([url['url'] for url in urls])
    idx, length = config.HOSTS.index(socket.gethostname()), len(urls)
    urls_shard = urls[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    print(socket.gethostname(), len(urls_shard))
    random.shuffle(urls_shard)
    for url in urls_shard:
        q_in.put(url)
    pools = []
    for _ in range(NUM_THREADS):
        pools.append(threading.Thread(target=other_error_thread, args=(q_in,)))
        pools[-1].start()
    for t in pools:
        t.join()


def dns_more_host_investigation():
    """
    For hosts with more than DNS Error problem, check whether they are still working
    """
    host_extractor = url_utils.HostExtractor()
    dns_error_hosts = db.host_status.aggregate([
        {"$match":{"year": year, "status": "DNSError"}},
        {"$lookup": {
            "from": "host_status",
            "let": {"hostname": "$hostname", "year": "$year"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$hostname", "$$hostname"]},
                    {"$eq": ["$year", "$$year"]}
                ]}}}
            ],
            "as": "meta"
        }},
        {"$match": {"meta.1": {"$exists" :True}}},
        {"$project": {"meta": False}}
    ])
    host_status = {}
    counter = 0
    def dns_thread_func(q_in, host_status):
        nonlocal counter
        while not q_in.empty():
            hostname = q_in.get()
            counter += 1
            print(counter, hostname)
            url = 'http://{}/'.format(hostname)
            try:
                r = requests.get(url, timeout=15)
            except:
                error_code = other_error(url)
                host_status[hostname] = error_code
                continue
            if r.status_code // 100 >= 4: 
                host_status[hostname] = '4/5xx'
            else:
                final_url = r.url
                if host_extractor.extract(final_url) == host_extractor.extract(url):
                    host_status[hostname] = 'same netloc'
                else:
                    host_status[hostname] = 'diff netloc'

    q_in = queue.Queue()
    for host in dns_error_hosts: q_in.put(host['hostname'])
    pools = []
    for _ in range(NUM_THREADS):
        pools.append(threading.Thread(target=dns_thread_func, args=(q_in, host_status)))
        pools[-1].start()
    for t in pools:
        t.join()
    json.dump(host_status, open('dns_host.json', 'w+'))


def sic_transit_test_200(NUM_THREADS=16):
    """
    Apply sic transit broken detection algorithm on 23xx status code pages
    """
    good_urls = db.url_status_implicit_broken.find({'status': re.compile("^[23]"), "hostname": "eventful.com"})
    count = 0
    def thread_func(q_in, i):
        nonlocal count
        update_ops = []
        while not q_in.empty():
            obj = q_in.get()
            url = obj['url']
            print(count, i, url)
            count += 1
            is_broken, reason = sic_transit.broken(url)
            if re.compile('^([45]|DNSError|OtherError)').match(reason): 
                print('Get non-200 status')
                continue
            update_ops.append(pymongo.UpdateOne(
                {"_id": obj['_id']}, 
                {"$set": {"sic_broken": is_broken, "sic_reason": reason}}
            ))
            if len(update_ops) >= 10:
                try: db.url_status_implicit_broken.bulk_write(update_ops, ordered=False)
                except: pass
                update_ops = []
        try: db.url_status_implicit_broken.bulk_write(update_ops, ordered=False)
        except: pass

    good_urls = list(good_urls)
    random.shuffle(good_urls)
    q_in = queue.Queue()
    for obj in good_urls:
        q_in.put(obj)
    pools = []
    for i in range(NUM_THREADS):
        pools.append(threading.Thread(target=thread_func, args=(q_in, i)))
        pools[-1].start()
    for t in pools:
        t.join()


if __name__ == '__main__':
    sic_transit_test_200(NUM_THREADS=5)