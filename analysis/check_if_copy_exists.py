from argparse import ArgumentParser
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

import sys

sys.path.append('..')

from utils import crawl, text_utils
from ReorgPageFinder.tools import Similar

import common
import json

class KeySet:
    def __init__(self, keys):
        self.keys = keys
        self.hashcode = sum([ hash(k) for k in keys ])

    def __hash__(self):
        return self.hashcode

    def __eq__(self, other):
        return self.keys == other.keys

def Main():
    archived_urls = get_archived_urls(args.archived_urls_filename)
    urls_to_find_copy = common.get_urls(args.urls_to_check_filename)
    for url_obj in urls_to_find_copy:
        url = url_obj['url']
        parsed_url = urlparse(url)
        removed_query = common.remove_query(parsed_url)
        url_obj['parsed'] = parsed_url
        if removed_query not in archived_urls:
            continue
        candidates = archived_urls[removed_query]
        candidate_intersections = find_candidate_intersection(url_obj, candidates)
        print('here')
        crawl_pages(candidate_intersections[0]['candidate']['wayback_url'],
                candidate_intersections[1]['candidate']['wayback_url'])
        break

def crawl_pages(first_url, second_url):
    print('Crawling {0} and {1}'.format(first_url, second_url))
    html1 = crawl.requests_crawl(first_url)
    html2 = crawl.requests_crawl(second_url)
    content1 = text_utils.extract_body(html1, version='domdistiller')
    content2 = text_utils.extract_body(html2, version='domdistiller')

    print('Finding similarity')
    similar = Similar()
    similar.tfidf._clear_workingset()
    similar.tfidf.add_corpus([content1, content2])
    similar_result = similar.tfidf.similar(content1, content2)
    print(similar_result)

def find_candidate_intersection(url_obj, candidates):
    url = url_obj['url']
    print('processing: ' + url)
    parsed_url = url_obj['parsed']
    orig_url_qs = parse_qs(parsed_url.query)
    stats = []
    for candidate in candidates:
        candidate_parsed_qs = candidate['parsed_qs']
        matched = []
        for key, values in orig_url_qs.items():
            # Skip if key is not in parsed query string.
            if key not in candidate_parsed_qs:
                continue
            candidate_param_values = candidate_parsed_qs[key]
            for value in values:
                try:
                    index = candidate_param_values.index(value)
                except ValueError as e:
                    continue
                matched.append((key, value))
        stats.append({
            'candidate': candidate,
            'matched': matched,
        })
    stats.sort(key=lambda x: len(x['matched']), reverse=True)
    return stats

def get_archived_urls(archived_urls_filename):
    '''Returns a dict mapping from URL --> candidates.'''
    with open(archived_urls_filename, 'r') as input_file:
        archived_urls = json.load(input_file)

    retval = defaultdict(list)
    for archived in archived_urls:
        url = archived['url']
        candidates = archived['candidates']
        for candidate in candidates:
            wayback_url = candidate['wayback_url']
            candidate_url = extract_url_from_wayback_url(wayback_url)
            parsed_candidate = urlparse(candidate_url)
            retval[url].append({
                'url': candidate_url,
                'wayback_url': wayback_url,
                'parsed': parsed_candidate,
                'parsed_qs': parse_qs(parsed_candidate.query),
            })
    return retval

def extract_url_from_wayback_url(wayback_url):
    start_idx = wayback_url.index('http', 1) # start the search skipping the first element.
    return wayback_url[start_idx:]

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('archived_urls_filename')
    parser.add_argument('urls_to_check_filename')
    args = parser.parse_args()
    Main()
