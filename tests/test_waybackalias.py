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
tr = None

def _init_large_obj():
    global simi, hist, tr
    if tr is None:
        try:
            os.remove(os.path.basename(__file__).split(".")[0] + '.log')
        except: pass
        logging.setLoggerClass(tracer.tracer)
        tr = logging.getLogger('logger')
        tr._unset_meta()
        tr._set_meta(os.path.basename(__file__).split(".")[0], db=db, loglevel=logging.DEBUG)
    if simi is None:
        simi = tools.Similar()
    if hist is None:
        hist = histredirector.HistRedirector(memo=memo)

def test_waybackalias_canfind():
    """URLs that should be found alias by wayback_alias"""
    _init_large_obj()
    url_alias = [
        ("http://www.atlassian.com:80/company/customers/case-studies/nasa", "https://www.atlassian.com/customers/nasa")
    ]
    for url, alias in url_alias:
        print(url)
        alias = hist.wayback_alias(url)
        assert(alias is not None)

def test_waybackalias_notfound():
    """URLs that should not be found alias by wayback_alias"""
    _init_large_obj()
    urls = [
        "http://www.intel.com:80/cd/corporate/europe/emea/eng/belgium/358249.htm",
        "https://www.meetup.com/1-Startup-Vitoria/messages/boards/forum/16297542/?sort=ThreadReplyCount&order=DESC",
        "http://www.att.com/accessories/es/specialty-items/gopro-gooseneck-mount-all-gopro-cameras.html?locale=es_US"
    ]
    for url in urls:
        print(url)
        alias = hist.wayback_alias(url)
        assert(alias is None)

unsolved = {
    "https://www.att.com/audio/ua-bluetooth-wireless-headphones-engineered-by-jbl.html" : False
}

def test_waybackalias_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "https://www.att.com/audio/ua-bluetooth-wireless-headphones-engineered-by-jbl.html"
    ]
    for url in urls:
        print(url)
        alias = hist.wayback_alias(url)
        assert(alias is None)
