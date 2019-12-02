"""
Intersect my url list with Jiangchen's redirection list
"""
import json
from os.path import join
from urllib.parse import urlparse, urlunparse

years = [2004, 2009, 2014, 2018]
file = open(join('data', 'link_year.json'), 'r')
data = json.load(file)
redir_data = {}

def url_complete(link):
    """
    Complete the link for: non-scheme, end slash
    """
    if "://" not in link:
        link = 'http://' + link
    parse_val = list(urlparse(link))
    # Add slash to path for homepage
    if parse_val[2] == "" or parse_val[2] == 'index.html':
        parse_val[2] = "/"
    return urlunparse(parse_val)


def intersect(year):
    dir_name = 'link_analysis_new_{}'.format(str(year))
    file = open(join('logs', dir_name, 'urls_2xx_to_homepage'), 'r').read().split('\n')
    while file[-1] == "":
        del file[-1]

    for line in file:
        origin_link, to_link = line.split()[0], line.split()[1]
        origin_link = url_complete(origin_link)
        if origin_link in data:
            redir_data[origin_link] = { "year": sorted(list(data[origin_link].keys()))[-1], "To": to_link}
    
    file = open(join('logs', dir_name, 'urls_redirect_to_non_homepage'), 'r').read().split('\n')
    while file[-1] == "":
        del file[-1]

    for line in file:
        origin_link, to_link = line.split()[0], line.split()[1]
        origin_link = url_complete(origin_link)
        if origin_link in data:
            redir_data[origin_link] = { "year": sorted(list(data[origin_link].keys()))[-1], "To": to_link}

for year in years:
    intersect(year)

out = open('data/meta_redirect_link_matching.json', 'w+')
json.dump(redir_data, out)
        
