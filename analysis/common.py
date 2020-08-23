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
            all_urls.append({
                'url': url,
                'broken_reason': broken_reason,
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

