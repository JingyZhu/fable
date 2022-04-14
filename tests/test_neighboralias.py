"""
Test all three techniques at once
"""
import pytest
import logging
import os
import json
import threading

from fable import tools, neighboralias, tracer, config
from fable.utils import url_utils

db = config.DB
nba = None
tr = None

def _init_large_obj():
    global tr, nba
    if tr is None:
        try:
            os.remove(os.path.basename(__file__).split(".")[0] + '.log')
        except: pass
        logging.setLoggerClass(tracer.tracer)
        tr = logging.getLogger('logger')
        logging.setLoggerClass(logging.Logger)
        tr._unset_meta()
        tr._set_meta(os.path.basename(__file__).split(".")[0], db=db, loglevel=logging.DEBUG)
    if nba is None:
        nba = neighboralias.NeighborAlias()

def test_get_neighbors():
    _init_large_obj()
    urls = [
        "http://www.metacritic.com/movie/ghostworld",
        "http://www.metacritic.com/movie/meangirls",
        "http://www.metacritic.com/movie/spiderman-3",
        "http://www.metacritic.com/movie/hail-caesar",
        "https://www.metacritic.com/movie/Zack-and-Meri-Make-a-Porno",
        "http://www.metacritic.com/movie/27dresses",
        "http://www.metacritic.com/movie/anacondasthehuntforthebloodorchid",
        "https://www.metacritic.com/movie/eyes-wide-open-",
        "http://www.metacritic.com/movie/the-water",
        "http://www.metacritic.com/movie/monalisasmile",
        "https://www.metacritic.com/movie/%D5%A5%D6%80%D5%AF%D6%80%D5%BA%D5%A1%D5%A3%D5%B8%D6%82%D5%B6%D5%A5%D6%80",
        "http://www.metacritic.com/movie/journeytothecenteroftheearth",
        "http://www.metacritic.com/movie/punisher2",
        "http://www.metacritic.com/movie/kiterunner",
        "http://www.metacritic.com/movie/three-colors-rede",
        "http://www.metacritic.com/movie/pirates-of-the-caribbean-dead-men-tell-non-tales"
  ]
    neighbors = nba.get_neighbors(urls, status_filter='2')
    print(len(neighbors))
    print(json.dumps(neighbors[:min(len(neighbors), 20)], indent=2))


def test_neighbor_aliases():
    _init_large_obj()
    urls = [
        "http://sharghnewspaper.ir/News/90/10/03/20282.html",
        "http://www.sharghnewspaper.ir/News/90/10/05/20431.html",
        "http://sharghnewspaper.ir/News/91/05/04/37859.html",
        "http://sharghnewspaper.ir/News/90/04/01/3223.html",
        "http://sharghnewspaper.ir/News/90/05/30/9012.html",
        "http://sharghnewspaper.ir/News/90/06/24/31885.html",
        "http://sharghnewspaper.ir/News/90/04/12/4229.html",
        "http://sharghnewspaper.ir/News/90/03/17/1804.html",
        "http://sharghnewspaper.ir/News/90/03/31/3163.html",
        "http://sharghnewspaper.ir/News/90/05/15/7457.html",
        "http://sharghnewspaper.ir/News/90/04/16/4723.html",
        "http://sharghnewspaper.ir/News/91/03/10/32986.html",
        "http://sharghnewspaper.ir/News/90/06/28/11529.html",
        "http://sharghnewspaper.ir/News/90/03/28/10532.html",
        "http://sharghnewspaper.ir/News/90/06/13/10000.html"
    ]
    sheet = nba.neighbor_aliases(urls, spec_method=['search', 'backlink_basic'], status_filter='2')
    print(json.dumps(sheet, indent=2))

test_get_neighbors()
# test_neighbor_aliases()