import pytest
import logging
import os
import json

from fable import tools, fable, tracer, config
from fable.utils import url_utils

he = url_utils.HostExtractor()
memo = tools.Memoizer()
simi = None
db = config.DB
alias_finder = None
tr = None

def _init_large_obj():
    global simi, alias_finder, tr
    if tr is None:
        try:
            os.remove(os.path.basename(__file__).split(".")[0] + '.log')
        except: pass
        logging.setLoggerClass(tracer.tracer)
        tr = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tr._unset_meta()
        tr._set_meta(os.path.basename(__file__).split(".")[0], db=db, loglevel=logging.DEBUG)
    if simi is None:
        simi = tools.Similar()
    if alias_finder is None:
        alias_finder = fable.AliasFinder(similar=simi)

def test_search_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "https://thewisesloth.com/2012/11/17/the-injustice-of-employee-contracts/",
        "https://thewisesloth.com/2010/09/19/voting-never-has-and-never-will-save-america/"
    ]
    aliases = alias_finder.search(urls)

    print(f'alias: {json.dumps(aliases, indent=2)}')


def test_hist_redir_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "http://www.foxnews.com/politics/2010/03/18/cornhusker-kickback-gets-boot-health",
        "http://www.foxnews.com/politics/2009/03/23/fusion-centers-expand-criteria-identify-militia-members/",
        "http://www.foxnews.com/politics/2011/01/19/christie-expands-number-charter-schools-new-jersey"
    ]
    aliases = alias_finder.hist_redir(urls)

    print(f'alias: {json.dumps(aliases, indent=2)}')


def test_verify_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "http://www.foxnews.com/politics/2010/03/18/cornhusker-kickback-gets-boot-health",
        "http://www.foxnews.com/politics/2009/03/23/fusion-centers-expand-criteria-identify-militia-members/",
        "http://www.foxnews.com/politics/2011/01/19/christie-expands-number-charter-schools-new-jersey"
    ]
    hr_cands = alias_finder.hist_redir(urls)
    se_cands = alias_finder.search(urls)
    cands = se_cands + hr_cands
    aliases = alias_finder.verify(urls, cands)

    print(f'alias: {json.dumps(aliases, indent=2)}')

test_verify_temp()