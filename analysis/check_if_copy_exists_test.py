from collections import defaultdict
from urllib.parse import urlencode

import unittest

import check_if_copy_exists

class TestCopyExists(unittest.TestCase):

    def test_order_archived(self):
        test = [
            check_if_copy_exists.Archived('https://foo.com/?k1=v1&k2=v2', ''),
            check_if_copy_exists.Archived('https://foo.com/?k1=v1', ''),
            check_if_copy_exists.Archived('https://foo.com/?k1=v1', ''),
            check_if_copy_exists.Archived('https://foo.com/?k2=v2', '')
        ]
        expected = [
            check_if_copy_exists.Archived('https://foo.com/?k1=v1', ''),
            check_if_copy_exists.Archived('https://foo.com/?k1=v1', ''),
            check_if_copy_exists.Archived('https://foo.com/?k1=v1&k2=v2', ''),
            check_if_copy_exists.Archived('https://foo.com/?k2=v2', '')
        ]
        self.assertSequenceEqual(sorted(test), expected)

    def test_get_any_two_candidates(self):
        base_url = 'https://foo.com/?'
        url_kv = [
            { 'k1': 'v1' },
            { 'k1': 'x' },
            { 'k1': 'y' },
            { 'k2': 'v2' },
            { 'k1': 'v1', 'k2': 'v2', 'k3': 'v3' },
            { 'k1': 'v1', 'k2': 'v3', 'k3': 'v3' },
            { 'k1': 'v1', 'k2': 'v4', 'k3': 'v3' }
        ]
        # setup the urls_with_key
        urls_with_key = construct_urls_with_key(base_url, url_kv)
        candidates = check_if_copy_exists.get_any_two_candidates({'k2'}, {'k1': 'v1'},
                urls_with_key)
        expected = (
            construct_archived_url(base_url,
                { 'k1': 'v1', 'k2': 'v2', 'k3': 'v3' },
            ),
            construct_archived_url(base_url,
                { 'k1': 'v1', 'k2': 'v3', 'k3': 'v3' },
            ),
        )
        self.assertTupleEqual(candidates, expected)

    def test_get_any_two_candidates_none(self):
        base_url = 'https://foo.com/?'
        url_kv = [
            { 'k1': 'v1' },
            { 'k1': 'x' },
            { 'k1': 'y' },
            { 'k2': 'v2' },
            { 'k1': 'v1', 'k2': 'v2', 'k3': 'v3' },
            { 'k1': 'v1', 'k2': 'v2' },
        ]
        # setup the urls_with_key
        urls_with_key = construct_urls_with_key(base_url, url_kv)
        candidates = check_if_copy_exists.get_any_two_candidates({'k2'}, {'k1': 'v1'},
                urls_with_key)
        self.assertIsNone(candidates)

    def test_get_urls_containing_all_kv_none(self):
        base_url = 'https://foo.com/?'
        url_kv = [
            { 'k1': 'v1' },
            { 'k1': 'x' },
            { 'k1': 'y' },
            { 'k2': 'v2' }
        ]
        # setup the urls_with_key
        urls_with_key = construct_urls_with_key(base_url, url_kv)
        urls = check_if_copy_exists.get_urls_containing_all_kv({'foo'}, {'k2': 'v2'},
                urls_with_key)
        self.assertSetEqual(urls, set())

    def test_get_urls_containing_all_kv_one_key(self):
        base_url = 'https://foo.com/?'
        url_kv = [
            { 'k1': 'v1' },
            { 'k1': 'x' },
            { 'k1': 'y' },
            { 'k2': 'v2' }
        ]
        # setup the urls_with_key
        urls_with_key = construct_urls_with_key(base_url, url_kv)
        urls = check_if_copy_exists.get_urls_containing_all_kv({'k1'}, {'k1': 'v1'},
                urls_with_key)
        expected = construct_url_set_for_kv(base_url, [
            { 'k1': 'v1' },
        ])
        self.assertSetEqual(urls, expected)

    def test_get_urls_containing_all_kv_two_keys(self):
        base_url = 'https://foo.com/?'
        url_kv = [
            { 'k1': 'v1' },
            { 'k1': 'x' },
            { 'k1': 'y' },
            { 'k1': 'v1', 'k2': 'v2', 'k3': 'v3' },
            { 'k1': 'v3', 'k2': 'v2', 'k3': 'v3' }
        ]
        # setup the urls_with_key
        urls_with_key = construct_urls_with_key(base_url, url_kv)
        urls = check_if_copy_exists.get_urls_containing_all_kv({ 'k1' },
        {
            'k2': 'v2',
            'k3': 'v3',
        }, urls_with_key)
        expected = construct_url_set_for_kv(base_url, [
            { 'k1': 'v1', 'k2': 'v2', 'k3': 'v3' },
            { 'k1': 'v3', 'k2': 'v2', 'k3': 'v3' }
        ])
        self.assertSetEqual(urls, expected)

    def test_get_urls_containing_all_kv_multiple_keys(self):
        base_url = 'https://foo.com/?'
        url_kv = [
            { 'k1': 'v1' },
            { 'k1': 'x' },
            { 'k1': 'y' },
            { 'k1': 'v1', 'k2': 'v2', 'k3': 'v3' },
            { 'k1': 'v1', 'k2': 'v2', 'k3': 'v3', 'k4': 'v4' },
            { 'k1': 'v3', 'k2': 'v2', 'k3': 'v3', 'k5': 'v5', 'k4': 'v4' }
        ]
        # setup the urls_with_key
        urls_with_key = construct_urls_with_key(base_url, url_kv)
        urls = check_if_copy_exists.get_urls_containing_all_kv({ 'k1' }, {
            'k2': 'v2',
            'k4': 'v4',
            'k3': 'v3'
        }, urls_with_key)
        expected = construct_url_set_for_kv(base_url, [
            { 'k1': 'v1', 'k2': 'v2', 'k3': 'v3', 'k4': 'v4' },
        ])
        self.assertSetEqual(urls, expected)

def construct_url(base_url, kv_dict):
    return base_url + urlencode(kv_dict)

def construct_archived_url(base_url, kv_dict):
    url = construct_url(base_url, kv_dict)
    archived_obj = check_if_copy_exists.Archived(url, url)
    return archived_obj

def construct_url_set_for_kv(base_url, url_kv):
    retval = set()
    for kv in url_kv:
        archived_obj = construct_archived_url(base_url, kv)
        retval.add(archived_obj)
    return retval

def construct_urls_with_key(base_url, url_kv):
    urls_with_key = defaultdict(set)
    for kv in url_kv:
        url = construct_url(base_url, kv)
        archived_obj = check_if_copy_exists.Archived(url, url)
        for k in kv:
            urls_with_key[k].add(archived_obj)
    return urls_with_key

if __name__ == '__main__':
    unittest.main()
