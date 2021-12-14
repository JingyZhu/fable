import pytest
import logging
import os

from fable import tools, histredirector, tracer, config
from fable.utils import url_utils

he = url_utils.HostExtractor()
memo = tools.Memoizer()
simi = None
db = config.DB
hist = None

def _init_large_obj():
    global simi, hist
    logging.setLoggerClass(tracer.tracer)
    tr = logging.getLogger('logger')
    tr._unset_meta()
    tr._set_meta(os.path.basename(__file__).split(".")[0], db=db, loglevel=logging.DEBUG)
    if simi is None:
        simi = tools.Similar()
    try:
        os.remove(os.path.basename(__file__).split(".")[0] + '.log')
    except: pass
    if hist is None:
        hist = histredirector.HistRedirector(memo=memo)

def test_waybackalias_notfound():
    """URLs that should not be found alias by wayback_alias"""
    _init_large_obj()
    urls = [
        "http://www.intel.com:80/cd/corporate/europe/emea/eng/belgium/358249.htm",
        "https://www.meetup.com/1-Startup-Vitoria/messages/boards/forum/16297542/?sort=ThreadReplyCount&order=DESC"
    ]
    for url in urls:
        print(url)
        alias = hist.wayback_alias(url)
        assert(alias is None)
