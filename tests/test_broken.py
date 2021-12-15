import pytest
import logging
import os

from fable import tools, tracer, config
from fable.utils import url_utils, sic_transit

he = url_utils.HostExtractor()
db = config.DB

def test_sictransit_isbroken():
    """URLs that should be broken"""
    urls = [
        "https://www.dartmouth.edu/wellness/new-location.html",
        "http://www.shopify.com/enterprise/44340803-3-common-misconceptions-about-conversion-rate-optimization-that-are-wasting-your-time?ad_signup=true&utm_source=cio&utm_medium=email&utm_campaign=digest_post_16d&utm_content=email_18",
        "https://www.att.com/es-us/accessories/specialty-items/gopro-gooseneck-mount-all-gopro-cameras.html"
    ]
    for url in urls:
        print(url)
        broken, _ = sic_transit.broken(url, html=True)
        assert(broken == True)