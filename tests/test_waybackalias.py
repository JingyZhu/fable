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
        ("http://www.bbc.co.uk:80/1xtra/djs/rampage/", "http://www.bbc.co.uk/1xtra/rampage/"),
        ("http://www.atlassian.com:80/company/customers/case-studies/nasa", "https://www.atlassian.com/customers/nasa"),
        ("https://www.docusign.com/esignature/electronically-sign", "https://www.docusign.com/products/electronic-signature"),
        ("http://www.starcitygames.com:80/magic/ravlimited/11682-The-Weekly-Guild-Build-What-About-Bob.html", "http://www.starcitygames.com/magic/ravlimited/11682_The_Weekly_Guild_Build_What_About_Bob.html")
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
        "http://www.att.com/accessories/es/specialty-items/gopro-gooseneck-mount-all-gopro-cameras.html?locale=es_US",
        "https://www.att.com/audio/ua-bluetooth-wireless-headphones-engineered-by-jbl.html",
        "http://www.skype.com:80/company/legal/terms/etiquette.html",
        "http://www.mediafire.com/?32qrp1eht670iiu",
        "http://www.dartmouth.edu:80/wellness/get_help/anthem_nurseline.html",
        "http://www.bbc.co.uk/5live/programmes/genres/sport/formulaone/current"
    ]
    for url in urls:
        print(url)
        alias = hist.wayback_alias(url)
        assert(alias is None)

unsolved = {
    "http://www.cdc.gov/24-7/savinglives/chickenpox/": False,
    "http://www.shopify.com:80/blog/15964292-3-common-misconceptions-about-conversion-rate-optimization-that-are-wasting-your-time?ad_signup=true&utm_source=cio&utm_medium=email&utm_campaign=digest_post_16d&utm_content=email_18": False
}

def test_waybackalias_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "http://www.cdc.gov/24-7/savinglives/chickenpox/"
    ]
    for url in urls:
        print(url)
        alias = hist.wayback_alias(url)
        assert(alias is None)
