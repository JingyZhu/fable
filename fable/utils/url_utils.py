
from publicsuffixlist import PublicSuffixList
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qsl, parse_qs, urlsplit, urlunsplit
import re
import os, time
from os.path import realpath, join, dirname
import subprocess
from collections import defaultdict
from url_normalize import url_normalize
from bs4 import BeautifulSoup
import sys

from fable import config

sys.setrecursionlimit(1500)

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

def get_ts(wayback_url):
    wayback_url = wayback_url.replace('http://web.archive.org/web/', '')
    url = wayback_url.replace('https://web.archive.org/web/', '')
    slash = url.find('/')
    ts = url[:slash]
    return ts

class urlset:
    def __init__(self, forms):
        """forms: form tag text on the html"""
        # TODO: implement this class constructor
        self.url = ''
        self.queries = []


class HostExtractor:
    def __init__(self):
        self.psl = PublicSuffixList()
    
    def extract(self, url, wayback=False):
        """
        Wayback: Whether the url is got from wayback
        """
        if wayback:
            url = filter_wayback(url)
        if 'http://' not in url and 'https://' not in url:
            url = 'http://' + url
        hostname = urlparse(url).netloc.split(':')[0]
        return self.psl.privatesuffix(hostname)

he = HostExtractor()


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


def url_match(url1, url2, wayback=True, case=False):
    """
    Compare whether two urls are identical on filepath and query
    If wayback is set to True, will first try to filter out wayback's prefix

    case: whether token comparison is token sensitive
    """
    if wayback:
        url1 = filter_wayback(url1)
        url2 = filter_wayback(url2)
    up1, up2 = urlparse(url1), urlparse(url2)
    netloc1, path1, query1 = up1.netloc.split(':')[0], up1.path, up1.query
    netloc2, path2, query2 = up2.netloc.split(':')[0], up2.path, up2.query
    if not case:
        netloc1, path1, query1 = netloc1.lower(), path1.lower(), query1.lower()
        netloc2, path2, query2 = netloc2.lower(), path2.lower(), query2.lower()
    netloc1, netloc2 = netloc1.split('.'), netloc2.split('.')
    if netloc1[0] == 'www': netloc1 = netloc1[1:]
    if netloc2[0] == 'www': netloc2 = netloc2[1:]
    if '.'.join(netloc1) != '.'.join(netloc2):
        return False
    if path1 == '': path1 = '/'
    if path2 == '': path2 = '/'
    if path1 != '/' and path1[-1] == '/': path1 = path1[:-1]
    if path2 != '/' and path2[-1] == '/': path2 = path2[:-1]
    dir1, file1 = os.path.split(path1)
    dir2, file2 = os.path.split(path2)
    if re.compile('^index').match(file1): path1 = dir1
    if re.compile('^index').match(file2): path2 = dir2
    if path1 != path2:
        return False
    if query1 == query2:
        return True
    qsl1, qsl2 = sorted(parse_qsl(query1), key=lambda kv: (kv[0], kv[1])), sorted(parse_qsl(query2), key=lambda kv: (kv[0], kv[1]))
    return len(qsl1) > 0 and qsl1 == qsl2


def url_norm(url, wayback=False, case=False, sort_query=True):
    """
    Perform URL normalization
    Namely, sort query by keys, and eliminate port number
    """
    if wayback:
        url = filter_wayback(url)
    us = urlsplit(url)
    path, query = us.path, us.query
    if not case:
        path, query = path.lower(), query.lower()
    us = us._replace(netloc=us.netloc.split(':')[0], fragment='')
    if path == '': 
        us = us._replace(path='/')
    if query and sort_query:
        qsl = sorted(parse_qsl(query), key=lambda kv: (kv[0], kv[1]))
        if len(qsl):
            us = us._replace(query='&'.join([f'{kv[0]}={kv[1]}' for kv in qsl]))
    return urlunsplit(us)


def url_parent(url, exclude_digit=False):
    """
    exclude_digit: When considering the parent. full digit tokens are filtered out
        This is used because full digit token are usually id, which are likely to be unique
    Return parent of url
    """
    us = urlsplit(url)
    site = he.extract(url)
    if us.path in ['', '/'] and not us.query:
        if site == us.netloc.split(':')[0]: return url
        hs = us.netloc.split(':')[0].split('.')
        return urlunsplit(us._replace(netloc='.'.join(hs[1:])))
    path = us.path
    if path and path [-1] == '/' and not us.query: path = path[:-1]
    if not exclude_digit:
        path = os.path.dirname(path)
    else:
        path = nondigit_dirname(path)
    return urlunsplit(us._replace(path=path, query=''))

def nondigit_dirname(path):
    """
    Return closest parent of URL where there is no digit token
    """
    if path not in ['', '/'] and path[-1] == '/':
        path = path[:-1]
    parts = path.split('/')
    parts = parts[:-1]
    while len(parts) and parts[-1].isdigit():
        parts = parts[:-1]
    return '/'.join(parts)
    

def is_parent(parent, url):
    """
    Check whether parent is truely a parent of url
    filename with ^index is considered as /

    Return Boolean
    """
    us, ps = urlsplit(url), urlsplit(parent)
    h1s, h2s = us.netloc.split(':')[0].split('.'), ps.netloc.split(':')[0].split('.')
    if h1s[0] == 'www': h1s = h1s[1:]
    if h2s[0] == 'www': h2s = h2s[1:]
    for h1, h2 in zip(reversed(h1s), reversed(h2s)):
        if h1 != h2: return False
    p1s, p2s = us.path, ps.path
    if p1s == '': p1s = '/'
    if p2s == '': p2s = '/'
    if p1s != '/' and p1s[-1] == '/': p1s = p1s[:-1]
    if p2s != '/' and p2s[-1] == '/': p2s = p2s[:-1]
    if len(h2s) != len(h1s):
        if len(h1s) - len(h2s) not in [0, 1] or p1s != p2s or us.query != ps.query:
            return False
    p1s, p2s = p1s.split('/')[1:], p2s.split('/')[1:]
    if re.compile('^index').match(p2s[-1]):
        p2s = p2s[:-1]
    for p1, p2 in zip(p1s, p2s):
        if p1 != p2: return False
    if len(p1s) - len(p2s) != 1:
        return False
    q1s, q2s = set(parse_qsl(us.query)), set(parse_qsl(ps.query))
    if len(q1s) == 0 and us.query or (len(q2s) == 0 and ps.query):
        return us.query == ps.query
    for q2 in q2s:
        if q2 not in q1s: return False
    return True


def gen_path_pattern(url, dis=1):
    """
    Generate path patterns where all paths with same edit distance should follow
    # TODO: Currently only support edit distance of 1,  Could have larger dis
    """
    us = urlsplit(url)
    us = us._replace(netloc=us.netloc.split(':')[0])
    if us.path == '':
        us = us._replace(path='/')
    if us.path[-1] == '/' and us.path != '/':
        us = us._replace(path=us.path[:-1])
    path_lists = list(filter(lambda x: x!= '', us.path.split('/')))
    if us.query: 
        path_lists.append(us.query)
    patterns = []
    patterns.append(tuple(['*'] + path_lists))
    for i in range(len(path_lists)):
        path_copy = path_lists.copy()
        path_copy[i] = '*'
        patterns.append(tuple([us.netloc] + path_copy))
    return patterns


def pattern_match(pattern, url, dis=1):
    us = urlsplit(url)
    us = us._replace(netloc=us.netloc.split(':')[0])
    if us.path == '':
        us = us._replace(path='/')
    if us.path[-1] == '/' and us.path != '/':
        us = us._replace(path=us.path[:-1])
    path_lists = list(filter(lambda x: x!= '', us.path.split('/')))
    path_lists = [us.netloc] + path_lists
    if us.query: 
        path_lists.append(us.query)
    if len(pattern) != len(path_lists):
        return False
    for pat, path in zip(pattern, path_lists):
        if pat == '*': continue
        elif pat != path: return False
    return True


def path_edit_distance(url1, url2):
    us1, us2 = urlsplit(url1), urlsplit(url2)
    dis = 0
    h1s, h2s = us1.netloc.split(':')[0].split('.'), us2.netloc.split(':')[0].split('.')
    if h1s[0] == 'www': h1s = h1s[1:]
    if h2s[0] == 'www': h2s = h2s[1:]
    if h1s != h2s:
        dis += 1
    path1 = list(filter(lambda x: x!= '', us1.path.split('/')))
    path2 = list(filter(lambda x: x!= '', us2.path.split('/')))
    for part1, part2 in zip(path1, path2):
        if part1 != part2: dis += 1
    dis += abs(len(path1) - len(path2))
    query1, query2 = sorted(parse_qsl(us1.query)), sorted(parse_qsl(us2.query))
    dis += (query1 != query2)
    return dis


def tree_diff(dest, src):
    seq1, seq2 = [], []
    us1, us2 = urlsplit(dest), urlsplit(src)
    h1s, h2s = us1.netloc.split(':')[0], us2.netloc.split(':')[0]
    seq1.append(h1s)
    seq2.append(h2s)
    p1s, p2s = us1.path, us2.path
    if p1s == '': p1s == '/'
    if p2s == '': p2s == '/'
    p1s, p2s = p1s.split('/'), p2s.split('/')
    seq1 += p1s[1:]
    seq2 += p2s[1:]
    diff = 0
    for i, (s1, s2) in enumerate(zip(seq1, seq2)):
        if s1 != s2: 
            diff += 1
            break
    diff += len(seq1) + len(seq2) - 2*(i+1)
    q1s, q2s = parse_qsl(us1.query), parse_qsl(us2.query)
    if diff == 0:
        diff += len(set(q1s).union(q2s)) - len(set(q1s).intersection(q2s))
    else:
        diff += min(len(q1s), len(set(q1s).union(q2s)) - len(set(q1s).intersection(q2s)))
    return diff


def common_prefix_diff(dest, src):
    """Distance from dest to common path prefix"""
    us1 = urlsplit(dest)
    us2 = urlsplit(src)
    p1s, p2s = us1.path, us2.path
    if p1s == '': p1s == '/'
    if p2s == '': p2s == '/'
    if len(p1s) > 1 and p1s[-1] == '/': p1s = p1s[:-1]
    if len(p2s) > 1 and p2s[-1] == '/': p2s = p2s[:-1]
    p1s, p2s = p1s.split('/')[1:], p2s.split('/')[1:]
    if us1.netloc.split(':')[0] != us2.netloc.split(':')[0]:
        return len(p1s) - 1
    i = 0
    for i, (s1, s2) in enumerate(zip(p1s, p2s)):
        if s1 != s2:
            i -= 1
            break
    i += 1
    return len(p1s) - i if i < len(p1s) else i - len(p2s)


def netloc_dir(url):
    url = filter_wayback(url)
    us = urlsplit(url)
    p = us.path
    if len(p) > 1 and p[-1] == '/': p = p[:-1]
    if p == '':  p == '/'
    return (us.netloc.split(':')[0], nondigit_dirname(p))
