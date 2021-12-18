import pytest
import logging
import os

from fable import tools, discoverer, tracer, config
from fable.utils import url_utils

he = url_utils.HostExtractor()
memo = tools.Memoizer()
simi = None
db = config.DB
dis = None
tr = None

def _init_large_obj():
    global simi, dis, tr
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
    if dis is None:
        dis = discoverer.Discoverer(memo=memo, similar=simi)

def test_backlink_withalias():
    """URLs that should be found alias by backlink"""
    _init_large_obj()
    url_alias = [
    ]
    for url, alias in url_alias:
        print(url)
        alias = dis.discover(url)
        assert(alias is not None)


def test_backlink_noalias():
    """URLs that should not be found alias by backlink"""
    _init_large_obj()
    urls = [
    ]
    for url in urls:
        print(url)
        site = he.extract(url)
        dis.similar._init_titles(site)
        alias = dis.discover(url)
        assert(alias is None)

unsolved = {

}

def test_backlink_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "http://www.ikea.com:80/aa/en/catalog/categories/departments/cooking/18846/"
    ]
    for url in urls:
        site = he.extract(url)
        dis.similar._init_titles(site)
        alias = dis.discover(url)
        tr.info(f'alias: {alias}')
        # assert(alias is None)