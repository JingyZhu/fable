"""
Check whether a page is broken
"""
import requests
import os
import shutil
from urllib.parse import urlparse 
from pymongo import MongoClient
import pymongo
import sys
import threading
import queue
import json
import socket
# from collections import defaultdict

sys.path.append('../')
import config

db = MongoClient(config.MONGO_HOSTNAME).web_decay
year = 1999
NUM_THREADS = 10
counter = 0

def send_request(url):
    '''
    Send one request to url.
    The documentation of various types of request errors is found here.

    https://2.python-requests.org/en/master/api/#exceptions
    '''

    resp = None
    user_agent_dict = {
        # the user agent
    }

    # TODO: look into the fail reason in ConnectionError and find which other ones are related to DNS.
    req_failed = True
    try:
        resp = requests.get(url, headers=user_agent_dict, timeout=10)
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
    except UnicodeError:
        error_msg = 'ERROR_UNICODE'
    except Exception as _:
        error_msg = 'ERROR_REQUEST_EXCEPTION_OCCURRED'

    if req_failed:
        return resp, error_msg

    return resp, 'SUCCESSFUL'


def test_links(q_in):
    '''Send requests to each link and log their status.'''
    global counter
    objs = []
    while not q_in.empty():
        url, hostname = q_in.get()
        counter += 1
        print(counter, url)
        resp, msg = send_request(url)
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
            else:
                status = 'OtherError'
                detail = msg
        objs.append({
            "_id": url,
            "url": url,
            "hostname": hostname,
            "year": year,
            "status": status,
            "detail": detail
        })
        if len(objs) >= 100:
            try:
                db.url_status.insert_many(objs, ordered=False)
            except Exception as e:
                print(str(e))
            objs = []
    try:
        db.url_status.insert_many(objs, ordered=False)
    except:
        pass
    

def sample_urls():
    """
    Sample urls from all hosts with added links >= N in year
    Put the sampled record into db.url_sample
    """
    db.url_sample.create_index([("hostname", pymongo.ASCENDING), ("year", pymongo.ASCENDING)])
    valid_hosts = list(db.hosts_added_links.find({'year': year, 'added_links': {'$gte': 500}}, {"hostname": True}))
    valid_hosts_in_year = []
    # Filter out later years' crawled hostname but has copies in this year
    for valid_host in valid_hosts:
        hostname = valid_host['hostname']
        if not db.hosts_meta.find_one({'hostname': hostname, 'year': year}):
            continue
        valid_hosts_in_year.append(hostname)
        valid_hosts_in_year = list(filter(lambda x: '\n' not in x, valid_hosts_in_year))
    for hostname in valid_hosts_in_year:
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
    hostnames = list(db.url_sample.aggregate([{'$group': {"_id": "$hostname"}}]))
    hostnames = sorted([obj['_id'] for obj in hostnames])
    idx, length = config.HOSTS.index(socket.gethostname()), len(hostnames)
    host_shards = hostnames[idx*length//len(config.HOSTS): (idx+1)*length//len(config.HOSTS)]
    for hostname in host_shards:
        urls_list = db.url_sample.find({'hostname': hostname, 'year': year})
        for obj in urls_list:
            q_in.put((obj['url'], hostname))
    pools = []
    for _ in range(NUM_THREADS):
        pools.append(threading.Thread(target=test_links, args=(q_in,)))
        pools[-1].start()
    for t in pools:
        t.join()

collect_status()