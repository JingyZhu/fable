import pytest
import logging
import os
import json

from fable import tools, fable, tracer, config, verifier
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
    if simi is None:
        simi = tools.Similar()
    if alias_finder is None:
        alias_finder = fable.AliasFinder(similar=simi, classname='test_fable', loglevel=logging.DEBUG)

def test_search():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "https://thewisesloth.com/2012/11/17/the-injustice-of-employee-contracts/",
        "https://thewisesloth.com/2010/09/19/voting-never-has-and-never-will-save-america/"
    ]
    aliases = alias_finder.search(urls)

    print(f'alias: {json.dumps(aliases, indent=2)}')


def test_hist_redir():
    """Temporary test to avoid long waiting for other tests"""
    _init_large_obj()
    urls = [
        "http://www.foxnews.com/politics/2010/03/18/cornhusker-kickback-gets-boot-health",
        # "http://www.foxnews.com/politics/2009/03/23/fusion-centers-expand-criteria-identify-militia-members/",
        # "http://www.foxnews.com/politics/2011/01/19/christie-expands-number-charter-schools-new-jersey"
    ]
    aliases = alias_finder.hist_redir(urls)

    print(f'alias: {json.dumps(aliases, indent=2)}')


def test_verify():
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


def test_neighbor_alias():
    _init_large_obj()
    urls = [
        "http://www.foxnews.com/politics/2010/03/18/cornhusker-kickback-gets-boot-health",
    ]
    hr_cands = alias_finder.hist_redir(urls)
    se_cands = alias_finder.search(urls)
    neighbor_urls, neighbor_aliases = alias_finder.get_neighbors(urls)
    cands = se_cands + hr_cands
    
    neighbor_cands = neighbor_aliases
    neighbor_cands += alias_finder.hist_redir(neighbor_urls)
    neighbor_cands += alias_finder.search(neighbor_urls)
    
    aliases = alias_finder.verify(urls, cands, neighbor_cands)

    print(f'alias: {json.dumps(aliases, indent=2)}')


def test_inference():
    _init_large_obj()

    urls = [
        "http://www.foxnews.com/politics/2009/12/26/lawmakers-attempted-airline-attack-disturbing-pledge-hold-hearings/",
    ]
    verified_cands = [
        [
        "http://www.foxnews.com/politics/2009/03/23/fusion-centers-expand-criteria-identify-militia-members/",
        [
          "'Fusion Centers' Expand Criteria to Identify Militia Members | Fox News"
        ],
        "https://www.foxnews.com/politics/fusion-centers-expand-criteria-to-identify-militia-members",
        {
          "method": "search",
          "type": "title",
          "value": 0.9062792647796163
        }
      ],
      [
        "http://www.foxnews.com/politics/2010/03/18/cornhusker-kickback-gets-boot-health",
        [
          "'Cornhusker' Out, More Deals In: Health Care Bill Gives Special Treatment | Fox News"
        ],
        "https://www.foxnews.com/politics/cornhusker-out-more-deals-in-health-care-bill-gives-special-treatment",
        {
          "method": "search",
          "type": "title",
          "value": 0.8857167321259989
        }
      ],
      [
        "http://www.foxnews.com/politics/2009/06/08/clinton-invites-controversial-muslim-leader-conference/",
        [
          "Clinton Invites Controversial Muslim Leader on Conference Call | Fox News"
        ],
        "https://www.foxnews.com/politics/clinton-invites-controversial-muslim-leader-on-conference-call",
        {
          "method": "search",
          "type": "title",
          "value": 0.8953564472540547
        }
      ]
    ]

    aliases = alias_finder.infer(urls, verified_cands)
    print(f'alias: {json.dumps(aliases, indent=2)}')



# test_hist_redir()
# test_neighbor_alias_temp()
test_inference()