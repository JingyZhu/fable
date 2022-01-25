import pytest
import logging
import os

from fable import config
from fable.utils import url_utils

he = url_utils.HostExtractor()
db = config.DB

def _diffs(url, alias):
    url_tokens = url_utils.tokenize_url(url, include_all=True)
    alias_tokens = url_utils.tokenize_url(alias, include_all=True)
    example_diffs = url_utils.url_token_diffs(url_tokens, alias_tokens)
    return tuple(sorted(e[:2] for e in example_diffs))

def test_url_token_diffs():
    same_diffs = [
        [
            ('http://pc.ign.com/articles/121/1212033p1.html', 'https://www.ign.com/articles/2011/11/10/the-elder-scrolls-v-skyrim-review'), 
            ('http://pc.ign.com/articles/808/808367p1.html', 'https://www.ign.com/articles/2007/07/26/gears-of-war-pc-qa'),
            ('http://pc.ign.com/articles/159/159942p1.html', 'https://www.ign.com/articles/1999/01/19/baldurs-gate-6')
        ]
    ]
    for examples in same_diffs:
        diffs = None
        for url, alias in examples:
            diff = _diffs(url, alias)
            if diffs is None: diffs = diff
            assert(diffs == diff)