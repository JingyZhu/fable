from argparse import ArgumentParser
from collections import defaultdict
from functools import total_ordering
from multiprocessing import Pool, Lock, Manager
from urllib.parse import urlparse, parse_qs

import sys

sys.path.append('..')

from utils import crawl, text_utils
from ReorgPageFinder.tools import Similar

import common
import logging
import json
import random

COMPARE_SUCCESS = 'success'
COMPARE_FAILED = 'failed'
COMPARE_UNCONCLUSIVE = 'unconclusive'

# TF-IDF threshold to declare a page content to be changing.
CONTENT_SIMILAR_THRESHOLD = 0.85

def Main():
    urls_to_find_copy = common.get_urls(args.urls_to_check_filename)
    archived_urls = get_archived_urls(args.archived_urls_filename,
            urls_to_find_copy)
    logging.debug('done getting archived_urls')

    # setup similar
    corpus_size = 100000
    if args.debug:
        corpus_size = 5
    similar = Similar(corpus_size=corpus_size)
    logging.debug('done similar()')

    results = []
    for url_obj in urls_to_find_copy:
        url = url_obj['url']
        logging.info('Processing: ' + url)
        url_removed_query = url_obj['removed_query']
        query_params = url_obj['query_params']
        if url_removed_query not in archived_urls:
            continue
        all_archived = archived_urls[url_removed_query]
        # Construct a mapping from k --> set(archived urls with k).
        urls_with_key = {}
        for archived in all_archived:
            for k in query_params:
                if archived.has_key_in_params(k):
                    if k not in urls_with_key:
                        urls_with_key[k] = set()
                    urls_with_key[k].add(archived)

        # We want to test each key one-by-one.
        non_influencing_keys = set()
        result = {
            'url': url,
            'result': COMPARE_FAILED,
        }
        for k, v in query_params.items():
            logging.debug('Testing: ' + k)
            testing = { k }

            # assumption: all keys except the testing keys are influecing keys
            # we then remove non-influencing keys in the next step using the
            # non_influencing_keys set.
            remaining_keys = query_params.keys() - testing
            remaining_kv = { k: v for k, v in query_params.items() \
                    if k in remaining_keys and k not in non_influencing_keys }
            # here, we can either
            #   1. return a copy, if we are able to find two URLs that contains
            #   the key k and exactly match on the remaining keys.
            urls_with_all_remaining_keys = \
                get_urls_containing_all_kv(testing, remaining_kv, urls_with_key)

            if len(urls_with_all_remaining_keys) >= 2:
                # we have enough urls to test the keys
                candidates = random.sample(urls_with_all_remaining_keys, 2)
                comparison_result = crawl_and_compare_pages(
                        similar, candidates[0].wayback_url,
                        candidates[1].wayback_url)
                if comparison_result['tf-idf'] >= CONTENT_SIMILAR_THRESHOLD:
                    # Got a candidate.
                    comparison_result['url'] = url
                    result = comparison_result
                    break

            # 2. conclude whether the key is an influencing key.
            else:
                candidates = get_any_two_candidates(
                        testing, remaining_kv, urls_with_key)
                if candidates is None:
                    # we can't test this query key. we are screwed.
                    break
                comparison_result = crawl_and_compare_pages(
                        similar, candidates[0].wayback_url,
                        candidates[1].wayback_url)
                if comparison_result['tf-idf'] >= CONTENT_SIMILAR_THRESHOLD:
                    non_influencing_keys.add(k)
        results.append(result)

    with open(args.output_filename, 'w') as output_file:
        output_file.write(json.dumps(results))

def get_any_two_candidates(keys_to_include, kv_to_match, urls_with_key):
    assert(len(keys_to_include) == 1) # we only test one key at a time.
    key_to_find_archived = [ k for k in keys_to_include ][0]
    if key_to_find_archived not in urls_with_key:
        return None

    archived_for_key = [ u for u in urls_with_key[key_to_find_archived] ]
    if len(archived_for_key) == 0:
        return None

    archived_for_key.sort()
    first_archived = archived_for_key[0]
    for i in range(1, len(archived_for_key)):
        second_archived = archived_for_key[i]
        if first_archived.kv_match(second_archived, ignore_keys={ key_to_find_archived }):
            return (first_archived, second_archived)
        first_archived = second_archived
    return None

def crawl_and_compare_pages(similar, first_url, second_url):
    '''Crawl and compare the given pages. It returns the following dictionary:
    {
        'first_url': first_url,
        'second_url': second_url,
        'tf-idf': similar_result,
        'k-shingling': text_utils.k_shingling(content1, content2),
    }
    '''
    logging.debug('Crawling {0} and {1}'.format(first_url, second_url))
    html1 = crawl.requests_crawl(first_url)
    html2 = crawl.requests_crawl(second_url)
    content1 = text_utils.extract_body(html1, version='domdistiller')
    content2 = text_utils.extract_body(html2, version='domdistiller')
    logging.debug('Got both content')
    similar.tfidf.add_corpus([content1, content2])
    similar_result = similar.tfidf.similar(content1, content2)
    try:
        k_shing = text_utils.k_shingling(content1, content2)
    except Exception as e:
        k_shing = -1

    comparison_result = {
        'result': COMPARE_SUCCESS,
        'first_url': first_url,
        'second_url': second_url,
        'tf-idf': similar_result,
        'k-shingling': k_shing,
    }
    logging.debug('tf-idf: ' + str(similar_result))
    return comparison_result

def get_urls_containing_all_kv(keys_to_include, kv_to_match, urls_with_key):
    assert(len(keys_to_include) == 1)
    keys_to_match_list = [ k for k in kv_to_match ]
    urls = None
    for i in range(0, len(keys_to_match_list)):
        key = keys_to_match_list[i]
        if key not in urls_with_key:
            continue

        if urls is None:
            urls = urls_with_key[key]
        else:
            urls &= urls_with_key[key]
    if urls is None:
        return set()

    key_to_include = [ u for u in keys_to_include ][0]
    final_urls = set()
    for url in urls:
        all_matched = True
        for k, v in kv_to_match.items():
            if not url.has_kv_in_params(k, v):
                all_matched = False
        all_matched &= not url.has_extra_keys(keys_to_include | kv_to_match.keys())
        all_matched &= url.has_key_in_params(key_to_include)
        if all_matched:
            final_urls.add(url)
    return final_urls

def get_archived_urls(archived_urls_filename, urls_to_find_copy):
    '''Returns a dict mapping from URL --> candidates.'''
    with open(archived_urls_filename, 'r') as input_file:
        archived_urls = json.load(input_file)

    to_find_copy_without_query = { common.remove_query(urlparse(u['url'])) for u in urls_to_find_copy }
    retval = defaultdict(list)
    for archived in archived_urls:
        url = archived['url']
        if url not in to_find_copy_without_query:
            continue

        candidates = archived['candidates']
        for candidate in candidates:
            wayback_url = candidate['wayback_url']
            candidate_url = common.extract_url_from_wayback_url(wayback_url)
            parsed_candidate = urlparse(candidate_url)
            retval[url].append(Archived(candidate_url, wayback_url))
    return retval

@total_ordering
class Archived(object):
    def __init__(self, url, wayback_url):
        self.url = url
        self.wayback_url = wayback_url
        self.parsed = urlparse(url)
        queries_list = parse_qs(self.parsed.query)
        self.parsed_qs = { k: v[0] for k, v in queries_list.items() }
        self.qs_key_order = sorted([ k for k in queries_list ])

    def has_key_in_params(self, k):
        return k in self.parsed_qs

    def has_kv_in_params(self, k, v):
        if not self.has_key_in_params(k):
            return False
        return v == self.parsed_qs[k]

    def has_extra_keys(self, keys):
        return len(self.parsed_qs.keys() - keys) > 0

    def kv_match(self, other, ignore_keys=set()):
        # can never match if the length does not match
        if len(self.parsed_qs) != len(other.parsed_qs):
            return False

        # can never match if the length does not match
        has_same_set_of_keys = len((self.parsed_qs.keys() |
            other.parsed_qs.keys()) - self.parsed_qs.keys()) == 0
        if not has_same_set_of_keys:
            return False

        # check for values
        for k, v in self.parsed_qs.items():
            # Skip key if we want to ignore this key.
            if k in ignore_keys:
                continue
            other_values = other.parsed_qs[k]
            if v != other_values:
                return False
        return True

    def __lt__(self, other):
        # an Archived object is ordered by the qs_key_order.
        cur_index = 0
        limit = min(len(self.qs_key_order), len(other.qs_key_order))
        while cur_index < limit:
            if self.qs_key_order[cur_index] == other.qs_key_order[cur_index]:
                key = self.qs_key_order[cur_index]
                if self.parsed_qs[key] != other.parsed_qs[key]:
                    return self.parsed_qs[key] < other.parsed_qs[key]
            else:
                return self.qs_key_order[cur_index] < other.qs_key_order[cur_index]
            cur_index += 1
        return len(self.qs_key_order) < len(other.qs_key_order)

    def __hash__(self):
        return hash(self.url)

    def __ne__(self, other):
        return self.url != other.url

    def __eq__(self, other):
        return self.url == other.url

    def __str__(self):
        return self.url

    def __repr__(self):
        return 'AR:' + self.url

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('archived_urls_filename')
    parser.add_argument('urls_to_check_filename')
    parser.add_argument('output_filename')
    parser.add_argument('--debug', action='store_true', default=False)
    args = parser.parse_args()
    if args.debug:
        print('setting to debug level')
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    Main()
