
from publicsuffix import fetch, PublicSuffixList
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qsl, urlsplit, urlunsplit
import re
import os, time
from os.path import realpath, join, dirname
import subprocess
from collections import defaultdict
from url_normalize import url_normalize

import sys
sys.path.append('../')
import config

def filter_wayback(url):
    if 'web.archive.org/web' not in url:
        return url
    url = url.replace('http://web.archive.org/web/', '')
    url = url.replace('https://web.archive.org/web/', '')
    slash = url.find('/')
    url = url[slash + 1:]
    return url

def constr_wayback(url, ts):
    if 'web.archive.org/web' in url:
        return url
    return f"http://web.archive.org/web/{ts}/{url}"

class urlset:
    def __init__(self, forms):
        """forms: form tag text on the html"""
        # TODO: implement this class constructor
        self.url = ''
        self.queries = []


class HostExtractor:
    def __init__(self):
        self.psl = PublicSuffixList(fetch())
    
    def extract(self, url, wayback=False):
        """
        Wayback: Whether the url is got from wayback
        """
        if wayback:
            url = filter_wayback(url)
        if 'http://' not in url and 'https://' not in url:
            url = 'http://' + url
        hostname = urlparse(url).netloc.split(':')[0]
        return self.psl.get_public_suffix(hostname)


class UrlRuleInferer:
    def __init__(self, tmp_path=config.TMP_PATH):
        self.path = tmp_path
        self.learned = False
        self.rule_dict = {}
        self.site = ""
        self.strans = join(dirname(realpath(__file__)), 'strans')
    
    def process_url(self, url):
        up = urlparse(url)
        return up.netloc.split(':')[0] + up.path + up.query
    
    def construct_match(self, url_pairs):
        file_string = ""
        for old_url, new_url in url_pairs:
            file_string += '{}=>{}\n'.format(old_url, new_url)
        return file_string
    
    def learn_rules(self, urls, site):
        """Learn rules for each url, same dir, and whole site
        url: list(tuple(old, new))"""
        self.site = site
        if self.learned:
            for v in self.rule_dict.values():
                try:os.remove(os.path.join(self.path, v))
                except: pass
            try: os.remove(os.path.join(self.path, 'rule_infer_list_' + self.site))
            except: pass
            self.rule_dict = {}
        dir_dict = defaultdict(list)
        site_urls = []
        for old_url, new_url in urls:
            old_url, new_url = self.process_url(old_url), self.process_url(new_url)
            filename = str(time.time()) + "_url_" + self.site
            subprocess.call([self.strans, '-b', old_url, '-a', new_url, '--save', join(self.path, filename)])
            self.rule_dict['url:' + old_url] = filename
            path_dir = dirname(urlparse(old_url).path)
            dir_dict[path_dir].append((old_url, new_url))
            site_urls.append((old_url, new_url))
        print("UrlRuleInferrer: url learned")
        for path_dir, path_urls in dir_dict.items():
            filename = str(time.time()) + "_dir_" + self.site
            match_str = self.construct_match(path_urls)
            f = open(join(self.path, 'rule_infer_list_' + self.site), 'w+')
            f.write(match_str)
            f.close()
            try:
                subprocess.call([self.strans, '-f',  join(self.path, 'rule_infer_list_' + self.site), '--save', join(self.path, filename)], timeout=10*60)
            except: continue
            self.rule_dict['dir:' + path_dir] = filename
        print("UrlRuleInferrer: dir learned")
        filename = str(time.time()) + "_site_" + self.site
        match_str = self.construct_match(site_urls)
        f = open(join(self.path, 'rule_infer_list_' + self.site), 'w+')
        f.write(match_str)
        f.close()
        subprocess.call([self.strans, '-f',  join(self.path, 'rule_infer_list_' + self.site), '--save', join(self.path, filename)])
        self.rule_dict['site'] = filename
        print("UrlRuleInferrer: site learned")
        self.learned = True
    
    def infer(self, url):
        inferred_urls = []
        url = self.process_url(url)
        path_dir = dirname(urlparse(url).path)
        for rule_name, rule_file in self.rule_dict.items():
            if rule_name[:4] in ['url:', 'site:'] or rule_name == 'dir:' + path_dir:
                try: output = subprocess.check_output('echo "{}" | {} --load {}'.format(url, self.strans, join(self.path, rule_file)), shell=True)
                except: continue
                inferred_urls.append(output.decode()[:-1])
        inferred_urls = list(filter(lambda x: x != '', inferred_urls))
        return list(set(inferred_urls))

    def __del__(self):
        print("deleted")
        if self.learned:
            for v in self.rule_dict.values():
                try:os.remove(os.path.join(self.path, v))
                except: pass
            try: os.remove(os.path.join(self.path, 'rule_infer_list_' + self.site))
            except: pass


def get_num_words(string):
    filter_str = ['', '\n', ' ']
    string_list = string.split()
    string_list = list(filter(lambda x: x not in filter_str, string_list))
    return ' '.join(string_list)


def find_link_density(html):
    """
    Find link density of a webpage given html
    """
    try:
        soup = BeautifulSoup(html, 'lxml')
    except:
        return 0
    filter_tags = ['style', 'script']
    for tag in filter_tags:
        for element in soup.findAll(tag):
            element.decompose()
    total_text = get_num_words(soup.get_text(separator=' '))
    total_length = len(total_text)
    atag_length = 0
    for atag in soup.findAll('a'):
        atag_text = get_num_words(atag.get_text())
        atag_length += len(atag_text)
    return atag_length / total_length if total_length != 0 else 0


def status_categories(status):
    """
    Given a detailed status code (or possibly its detail)
    Return a better categorized status
    Consider status = list of string as Soft-404
    Soft-404/ 4/5xx / DNSErrorOtherError
    """
    # if not re.compile("^([2345]|DNSError|OtherError)").match(status): return "Unknown"
    if re.compile("^[45]").match(status): return "4/5xx"
    elif re.compile("^(DNSError|OtherError)").match(status): return "DNSOther"
    elif  re.compile("^(\[.*\]|Similar|Same|no features)").match(status): return "Soft-404"
    else:
        return status


def url_match(url1, url2, wayback=True):
    """
    Compare whether two urls are identical on filepath and query
    If wayback is set to True, will first try to filter out wayback's prefix
    """
    if wayback:
        url1 = filter_wayback(url1)
        url2 = filter_wayback(url2)
    up1, up2 = urlparse(url1), urlparse(url2)
    netloc1, path1, query1 = up1.netloc.split(':')[0], up1.path, up1.query
    netloc2, path2, query2 = up2.netloc.split(':')[0], up2.path, up2.query
    if netloc1 != netloc2:
        return False
    if path1 == '': path1 = '/'
    if path2 == '': path2 = '/'
    if path1 != path2:
        return False
    if query1 == query2:
        return True
    qsl1, qsl2 = sorted(parse_qsl(query1), key=lambda kv: (kv[0], kv[1])), sorted(parse_qsl(query2), key=lambda kv: (kv[0], kv[1]))
    return len(qsl1) > 0 and qsl1 == qsl2


def url_norm(url, wayback=False):
    """
    Perform URL normalization
    Namely, sort query by keys, and eliminate port number
    """
    if wayback:
        url = filter_wayback(url)
    us = urlsplit(url)
    path, query = us.path, us.query
    us = us._replace(netloc=us.netloc.split(':')[0])
    if path == '': 
        us = us._replace(path='/')
    if query:
        qsl = sorted(parse_qsl(query), key=lambda kv: (kv[0], kv[1]))
        us = us._replace(query='&'.join([f'{kv[0]}={kv[1]}' for kv in qsl]))
    return urlunsplit(us)