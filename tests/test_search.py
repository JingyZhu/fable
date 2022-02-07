import pytest
import logging
import os
import json

from fable import tools, searcher, tracer, config
from fable.utils import url_utils

he = url_utils.HostExtractor()
memo = tools.Memoizer()
simi = None
db = config.DB
search = None
tr = None

def _init_large_obj():
    global simi, search, tr
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
    if search is None:
        search = searcher.Searcher(memo=memo, similar=simi)

def test_search_withalias():
    """URLs that should be found alias by search"""
    _init_large_obj()
    url_alias = [
        ("http://rpgvault.ign.com/articles/875/875540p1.html", "https://www.ign.com/articles/2008/05/21/rpg-vault-focus-russia-part-1"),
        ("http://www.wiley.com:80/cda/product/0,,0471357278,00.html", "https://www.wiley.com/en-us/Principles+of+Molecular+Mechanics-p-9780471357278")
    ]
    for url, alias in url_alias:
        print(url)
        site = he.extract(url)
        search.similar._init_titles(site)
        alias = search.search(url, search_engine='bing')
        if alias is None:
            alias = search.search(url, search_engine='google')
        assert(alias[0] is not None)


def test_search_noalias():
    """URLs that should not be found alias by search"""
    _init_large_obj()
    urls = [
        "http://toxnet.nlm.nih.gov/cgi-bin/sis/htmlgen?HSDB",
        "https://www.tensorflow.org/api_docs/python/tf/keras/layers/CuDNNLSTM",
        "https://asciinema.org/a/2znuSiyoonwpDB7QriovrrC0V",
        "https://jobs.cigna.com/us/en/job/21011897/Senior-Supplemental-Health-Compliance-Analyst"
    ]
    for url in urls:
        print(url)
        site = he.extract(url)
        search.similar._init_titles(site)
        alias = search.search(url, search_engine='bing')
        if alias is None:
            alias = search.search(url, search_engine='google')
        assert(alias is None)

unsolved = {
    # ! Title tweaked, no subset
    "http://www.consumerreports.org:80/cro/appliances/kitchen-appliances/coffeemakers/pod-coffeemaker-ratings/models/price-and-shop/buy-keurig-k45-elite-brewing-system-99048951.htm": True,
    # ! URL Token same (after tokenization)
    "http://www.abc.net.au:80/100years/EP2_4.htm": False
}

def test_search_temp():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "https://www.wthr.com/article/guilty-plea-1988-murder-8-year-old-ft-wayne-girl"
    ]
    results = []
    for url in urls:
        site = he.extract(url)
        search.similar._init_titles(site)
        alias = search.search(url, search_engine='bing')
        if alias[0] is None:
            alias = search.search(url, search_engine='google')
        tr.info(f'alias: {alias}')
        assert(alias[0] is None)
        # results.append({'url': url, 'alias': alias})
        # json.dump(results, open('test_search.json', 'w+'), indent=2)