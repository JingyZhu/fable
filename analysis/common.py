from urllib.parse import urlparse, parse_qs

import json

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

