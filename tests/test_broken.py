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
        "https://www.att.com/es-us/accessories/specialty-items/gopro-gooseneck-mount-all-gopro-cameras.html"
    ]
    for url in urls:
        print(url)
        broken, _ = sic_transit.broken(url, html=True)
        assert(broken == True)