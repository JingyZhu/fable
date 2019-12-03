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

def article_date(html):
    """
    Get the publish date of a webpage using article library
    Return datetime.datetime
    """
    article = Article(url='http://localhost:8080')
    article.download()
    article.html = html
    article.parse()
    return article.publish_date


def mine_date(html):
    """
    Mine way of trying to get date
    """
    soup = BeautifulSoup(html, 'html.parser')
    dates = set()
    tag_list = ['p', 'div'] + ['h{}'.format(i) for i in range(1, 7)]
    for tag in tag_list:
        for piece in soup.find_all(tag):
            try:
                dt = dparser.parse(piece.text, fuzzy=True)
            except: 
                continue
            dt = dt.strftime("%Y %m %d")
            dates.add((dt, len(piece.text.split())))
    dates = sorted([o for o in dates], key=lambda x: x[1])
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
