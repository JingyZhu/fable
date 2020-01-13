
from publicsuffix import fetch, PublicSuffixList
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re

class HostExtractor:
    def __init__(self):
        self.psl = PublicSuffixList(fetch())
    
    def extract(self, url, wayback=False):
        """
        Wayback: Whether the url is got from wayback
        """
        if wayback:
            url = url.replace('http://web.archive.org/web/', '')
            url = url.replace('https://web.archive.org/web/', '')
            slash = url.find('/')
            url = url[slash + 1:]
        if 'http://' not in url and 'https://' not in url:
            url = 'http://' + url
        hostname = urlparse(url).netloc.split(':')[0]
        return self.psl.get_public_suffix(hostname)


def filter_separator(string):
    separator = [' ', '\n']
    for sep in separator:
        string = string.replace(sep, '')
    return string


def find_link_density(html):
    """
    Find link density of a webpage given html
    """
    soup = BeautifulSoup(html, 'html.parser')
    filter_tags = ['style', 'script']
    for tag in filter_tags:
        for element in soup.findAll(tag):
            element.decompose()
    total_text = filter_separator(soup.get_text())
    total_length = len(total_text)
    atag_length = 0
    for atag in soup.findAll('a'):
        atag_text = filter_separator(atag.get_text())
        atag_length += len(atag_text)
    return atag_length / total_length if total_length != 0 else 0


def status_categories(status, detail):
    """
    Given a detailed status code (and possibly its detail)
    Return a better categorized status
    no redirection/ homepage/ non-homepage/ 4/5xx / DNSError / OtherError_Type
    """
    if not re.compile("^([2345]|DNSError|OtherError)").match(status): return "Unknown"
    if re.compile("^[45]").match(status): return "4/5xx"
    elif re.compile("^[23]").match(status): return detail
    elif re.compile("^DNSError").match(status): return status
    elif  re.compile("^OtherError").match(status): return "OtherError_" + detail
    else:
        raise


