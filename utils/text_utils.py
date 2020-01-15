"""
Utils for text
"""
import justext
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
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

class TFidf:
    def re_init(self):
        """
        Re calculated the tfidf from the self.corpus
        """
        self.vectorizer = TfidfVectorizer()
        self.tfidf = self.vectorizer.fit_transform(self.corpus)
        self.simi = cosine_similarity(self.tfidf)

    def __init__(self, corpus):
        corpus = list(set(corpus))
        self.idx = {c: i for i, c in enumerate(corpus)}
        self.corpus = corpus
        self.vectorizer = TfidfVectorizer()
        self.tfidf = self.vectorizer.fit_transform(corpus)
        self.simi = cosine_similarity(self.tfidf)
    
    def similar(self, text1, text2):
        """
        Get similarity of 2 text
        If any of the text is not in the corpus, TFIDF matrix will be recalculated
        """
        need_reinit = False
        if text1 not in self.idx:
            self.idx[text1] = len(self.corpus)
            self.corpus.append(text1)
            need_reinit = True
        if text2 not in self.idx:
            self.idx[text2] = len(self.corpus)
            self.corpus.append(text2)
            need_reinit = True
        if need_reinit: self.re_init()
        return self.simi[self.idx[text1], self.idx[text2]]
    
    def topN(self, text, N=5):
        """
        Get the highest weighted N words in a text
        If text is not in the corpus, it'll be added, and tfidf'll be recalculated
        """
        need_reinit = False
        if text not in self.idx:
            self.idx[text] = len(self.corpus)
            self.corpus.append(text)
            need_reinit = True
        if need_reinit: self.re_init()
        array = self.tfidf[self.idx[text]].toarray()[0]
        idxes = array.argsort()[-N:]
        words = self.vectorizer.get_feature_names()
        return [words[i] for i in reversed(idxes)]


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
    try:
        stoplist = justext.get_stoplist(lang)
    except:
        stoplist = justext.get_stoplist("English")
    paragraphs = justext.justext(html, stoplist)
    text = ''
    for p in paragraphs:
        if not p.is_boilerplate:
            text += ' ' + p.text
    return text


def newspaper_extract(html):
    article = Article('https://google.com') # Dummy urls to initialize the obj Can be anything able to wget
    article.download(input_html=html)
    article.parse()
    return article.text


def extract_body(html, version='justext'):
    """
    Wrapper functions for different version of html body extraction
    """
    if html == '': return ''
    func_dict = {
        "justext": justext_extract,
        "goose": goose_extract,
        "newspaper": newspaper_extract
    }
    return func_dict[version](html)


def newspaper_title_extract(html):
    article = Article('https://localhost:8988')
    article.download(input_html=html)
    article.parse()
    return article.title


def mine_title_extract():
    # TODO Inplement this func
    pass


def extract_title(html, version='mine'):
    """
    Wrapper functions for different version of html body extraction
    """
    if html == '': return ''
    func_dict = {
        "newspaper": newspaper_title_extract,
        "mine": mine_title_extract
    }
    return func_dict[version](html)
