from functools import total_ordering
from urllib.parse import urlparse, parse_qs

import json

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

    def has_params(self):
        return len(self.parsed_qs) > 0

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



def get_urls(filename):
    '''Returns a list of URLs and their corresponding broken reason gathered
    from the provided file.

    This function expects the input file to be in the following format:
    {
        broken_reason_1: [
            ...urls...
        ],
        broken_reason_2: [
            ...urls...
        ]
    }
    '''
    with open(filename, 'r') as input_file:
        file_obj = json.load(input_file)
    all_urls = []
    for broken_reason, urls in file_obj.items():
        for url in urls:
            url = url.replace(':80', '')
            parsed_url = urlparse(url)
            queries_list = parse_qs(parsed_url.query)

            # collapse list into just dictionary.
            queries = { k: v[0] for k, v in queries_list.items() }
            all_urls.append({
                'url': url,
                'parsed': parsed_url,
                'broken_reason': broken_reason,
                'query_params': queries,
                'removed_query': remove_query(parsed_url)
            })
    return all_urls

def has_query(parsed_url):
    '''Returns whether the given URL contains either parameter, query string, or
    fragments.'''
    return len(parsed_url.params) > 0 or \
            len(parsed_url.query) > 0 or \
            len(parsed_url.fragment) > 0

def remove_query(parsed_url):
    '''Returns a string of the given URL but components after the path
    removed.'''
    return '{scheme}://{hostname}{path}'.format(
            scheme=parsed_url.scheme,
            hostname=parsed_url.netloc,
            path=parsed_url.path
    )

def extract_url_from_wayback_url(wayback_url):
    start_idx = wayback_url.index('http', 1) # start the search skipping the first element.
    return wayback_url[start_idx:]

