from argparse import ArgumentParser
from collections import defaultdict
from multiprocessing import Pool, Lock
from urllib.parse import urlparse

import common
import sys

sys.path.append('..')

import json
import logging

from utils import crawl

def Main():
    all_urls = common.get_urls(args.page_list)
    results = []

    pool = Pool()
    apply_results = []
    seen_removed_query = set()
    urls_to_process = 0
    for url_obj in all_urls:
        parsed_url = urlparse(url_obj['url'])
        if not common.has_query(parsed_url):
            # Skip URLs without query parameters for now.
            continue

        removed_query = common.remove_query(parsed_url)
        url_obj['removed_query'] = removed_query
        if removed_query in seen_removed_query:
            # don't do repeated work.
            continue
        seen_removed_query.add(removed_query)
        apply_result = pool.apply_async(process_url, args=(url_obj,))
        apply_results.append(apply_result)
        urls_to_process += 1
    pool.close()
    pool.join()

    print('URLs to process: ' + str(urls_to_process))
    # Get the results from processes.
    for apply_result in apply_results:
        url_result = apply_result.get()
        results.append(url_result)

    # Write the result to file.
    with open(args.output_filename, 'w') as output_file:
        output_file.write(json.dumps(results))

def process_url(url_obj):
    '''Process a URL and return an object containing the query result from
    Wayback Machine.
    '''
    url = url_obj['removed_query']
    print('Processing: ' + url)
    parsed_url = urlparse(url)
    candidates = check_url(parsed_url)
    return {
        'url': url,
        'candidates': candidates,
        'broken_reason': url_obj['broken_reason'],
    }

def check_url(parsed_url):
    '''Returns a list of URL candidates.'''
    removed_query = common.remove_query(parsed_url)
    removed_query += '*' # Make this a prefix search.
    # Restrict the results to only 200 status code.
    cdx_query_params = {
        'filter': 'statuscode:200'
    }
    archived_candidates, _ = crawl.wayback_index(removed_query,
            total_link=True,
            param_dict=cdx_query_params)
    result = [ transform_candidate(c) for c in archived_candidates ]
    result = [ c for c in result if c['status_code'] == '200' ]
    if len(result) == 0:
        return []
    return get_latest_snapshot(result)

def get_latest_snapshot(candidates):
    '''Returns a list of candidates that are the latest snapshot for each of the
    candidate.'''
    url_to_candidates = defaultdict(list)
    for candidate in candidates:
        url = common.extract_url_from_wayback_url(candidate['wayback_url'])
        url_to_candidates[url].append(candidate)

    retval = []
    for url, url_candidates in url_to_candidates.items():
        url_candidates.sort(key=lambda x: x['timestamp'], reverse=True)
        retval.append(url_candidates[0])
    return retval

def transform_candidate(candidate):
    '''Each candidate is passed in the format described in utils/crawl.py:

        (timestamp, url, status_code)

    This function converts the tuple format to a more readable object.
    '''
    return {
        'timestamp': candidate[0],
        'wayback_url': candidate[1],
        'status_code': candidate[2],
    }

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('page_list')
    parser.add_argument('output_filename')
    parser.add_argument('--debug', action='store_true', default=False)
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.INFO)
    Main()
