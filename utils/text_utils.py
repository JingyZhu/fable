"""
Utils for text
"""
import justext
from langcodes import Language
from langdetect import detect_langs
from goose3 import Goose
from newspaper import Article
import brotli
from bs4 import BeautifulSoup
from dateutil import parser as dparser
from dateparser.search import search_dates
import dateparser
import difflib

def find_complement_string(A, B):
    A, B = A.split(), B.split()
    complement = []
    ida, idb = 0, 0
    while ida < len(A):
        if idb < len(B) and A[ida] == B[idb]:
            ida += 1
            idb += 1
        else:
            complement.append(A[ida])
            ida += 1
    return ' '.join(complement)


def article_date(html):
    """
    Get the publish date of a webpage using article library
    Return datetime.datetime
    """
    article = Article(url='')
    article.set_html(html)
    article.parse()
    return article.publish_date


def mine_date(html):
    """
    Mine way of trying to get date
    """
    soup = BeautifulSoup(html, 'html.parser')
    dates = set()
    filter_tags = ['script', 'a', 'style']
    for tag in filter_tags:
        for certain_tag in soup.findAll(tag):
            certain_tag.decompose()
    tag_list = ['div', 'p', 'span', 'b'] + ['h{}'.format(i) for i in range(1, 7)]
    for tag in tag_list:
        for piece in soup.find_all(tag):
            if len(piece.find_all()) > 1 : # Not leaf node
                continue
            # First try dateutils
            text = piece.text
            try:
                dt, tokens = dparser.parse(text, fuzzy_with_tokens=True)
            except:
                continue
            # Get the date part
            tokens = ''.join(tokens)
            date_str = find_complement_string(text, tokens)
            # Test on dateparser
            try:
                dt = dateparser.parse(date_str + ' ', settings={'STRICT_PARSING': True})
            except:
                continue
            if dt is None:
                continue
            dt = dt.strftime("%Y %m %d")
            dates.add((dt, len(piece.text.split())))
    for time_tag in soup.find_all('time'):
        if 'datetime' in time_tag.attrs:
            dt = time_tag.attrs['datetime']
            dt = dparser.parse(dt, fuzzy=True).strftime("%Y %m %d")
            dates.add((dt, 0))
    dates = sorted([o for o in dates], key=lambda x: x[1])
    print(dates)
    return dates[0][0] if len(dates) > 0 else ""


def extract_date(html, version="article"):
    """
    Wrapper function for different version of date extraction
    """
    func_dict = {
        "mine": mine_date,
        "article": article_date
    }
    return func_dict[version](html)

def brotli_compress(html):
    """
    Compress html to brotoli
    """
    return brotli.compress(html.encode())

def brotli_decompree(compressed):
    return brotli.decompress(compressed).decode()


def goose_extract(html):
    g = Goose()
    article = g.extract(raw_html=html)
    return article.cleaned_text


def justext_extract(html):
    lang_code = detect_langs(html)[0].lang
    lang = Language.make(language=lang_code).language_name()
    paragraphs = justext.justext(html, justext.get_stoplist(lang))
    text = ''
    for p in paragraphs:
        if not p.is_boilerplate:
            text += ' ' + p.text
    return text


def extract_body(html, version='justext'):
    """
    Wrapper functions for different version of html body extraction
    """
    func_dict = {
        "justext": justext_extract,
        "goose": goose_extract

    }
    return func_dict[version](html)
