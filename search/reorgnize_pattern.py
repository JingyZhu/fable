"""
Script for finding the pattern of url reorgnization
"""
import requests
import json
from urllib.parse import urlparse, parse_qsl
import difflib
import os

def find_filename(path):
    if path == '': path = '/'
    elif path[-1] == '/' and path != '/':
        path = path[:-1]
    filename = os.path.basename(path)
    return filename
    

def same_file_path():
    """
    See how many of the filename + query are the same
    """
    data = json.load(open('../tmp/reorganize.json', 'r'))
    total_searched, same_fq = 0, 0
    same_f, same_q, not_same = 0, 0, 0
    same_dict = {'file query': [], 'file': [], 'query': [], 'not same': []}
    for objs in data.values():
        for url in objs:
            if 'searched_url' not in url:
                continue
            total_searched += 1
            orig_up, se_up = urlparse(url['url']), urlparse(url['searched_url'])
            orig_file, orig_query = find_filename(orig_up.path), parse_qsl(orig_up.query)
            se_file, se_query = find_filename(se_up.path), parse_qsl(se_up.query)
            if orig_file == se_file and se_query == orig_query:
                same_dict['file query'].append(url)
                same_fq += 1
            elif orig_file == se_file:
                same_dict['file'].append(url)
                same_f += 1
            elif orig_query == se_query:
                same_dict['query'].append(url)
                same_q += 1
                print(orig_file, se_file)
            else:
                same_dict['not same'].append(url)
                not_same += 1
    print(total_searched, same_fq, same_f, same_q, not_same)
    json.dump(same_dict, open('../tmp/file_query.json', 'w+'))


same_file_path()