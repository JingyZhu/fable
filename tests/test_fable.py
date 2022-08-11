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
        "https://thewisesloth.com/2012/11/17/the-injustice-of-employee-contracts/"
    ]
    for url in urls:
        aliases = alias_finder.search(url)

        print(f'alias: {json.dumps(aliases, indent=2)}')

test_search_temp()