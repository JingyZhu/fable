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
from boilerpipe.extract import Extractor
import brotli
from bs4 import BeautifulSoup
from dateutil import parser as dparser
from dateparser.search import search_dates
import dateparser, difflib
from os.path import join, dirname, abspath, splitext
from subprocess import call, Popen, check_output
import re, os, time
import sys, copy
import multiprocessing as mp
import scipy.sparse as sp
import numpy as np
from collections import defaultdict

sys.path.append('../')
from utils import url_utils
try:
    import config
except:
    print("No config.py, Specify you own port")

# Need to be modified
tmp_path = config.TMPPATH
PORT = config.LOCALSERVER_PORT # If no config.py, modify to self chosen port
# Need to be modified

NULL = open('/dev/null', 'w')
def localserver(PORT):
    """
    Create tmp dir at $PROJ_HOME, copy domdistiller.js into the repo
    Serve a local server at port if it not occupied by any others
    """
    cur_path = dirname(__file__)
    call(['mkdir', '-p', tmp_path])
    call(['cp', join(cur_path, 'domdistiller.js'), tmp_path])
    port_occupied = re.compile(":{}".format(PORT)).findall(check_output(['netstat', '-nlt']).decode())
    if len(port_occupied) <= 0:
        Popen(['http-server', '-a', 'localhost', '-p', str(PORT), tmp_path], stdout=NULL, stderr=NULL)
    else:
        print("Port {} occupied by other process".format(PORT))

localserver(PORT)

class TFidfDynamic:
    def re_init(self):
        """
        Re calculated the tfidf from the self.corpus
        """
        print("re_init")
        # self.vectorizer = TfidfVectorizer()
        self.tfidf = self.vectorizer.fit_transform(self.corpus)
        # print(f"Takes {(time.time()-begin):.2f}s")

    def __init__(self, corpus):
        corpus = list(set(corpus))
        self.idx = {c: i for i, c in enumerate(corpus)}
        self.corpus = corpus
        self.vectorizer = TfidfVectorizer()
        self.tfidf = self.vectorizer.fit_transform(corpus)

    def similar(self, text1, text2):
        """
        Get similarity of 2 text
        If any of the text is not in the corpus, TFIDF matrix will be recalculated
        """
        if text1 == "" or text2 == "":
            return 0
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
        idx1, idx2 = self.idx[text1], self.idx[text2]
        return cosine_similarity(self.tfidf[idx1].toarray(), self.tfidf[idx2].toarray())[0,0]
    
    def topN(self, text, N=10):
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
    
    def add_corpus(self, corpus):
        need_reinit = False
        new_c = [c for c in corpus if c not in self.idx]
        for c in new_c:
            need_reinit = True
            self.idx[c] = len(self.corpus)
            self.corpus.append(c)
        if need_reinit: self.re_init()


class TFidfStatic:
    def __init__(self, corpus):
        corpus = list(set(corpus))
        self.corpus = corpus
        self.vectorizer = TfidfVectorizer()
        self.vectorizer.fit(corpus)
        self.tfidf = self.vectorizer.transform(corpus)
        self.workingset_vec = None
        self.workingset_tfidf = None
        self.idx = None
    
    def _init_workingset(self, inputs):
        """
        Get tfidf within inputs. 
        TFIDF value will be based on the previous corpus instead of the input
        """
        inputs = list(set(inputs))
        self.idx = {i: c for c, i in enumerate(inputs)}
        # Get vocabulary from inputs
        idf = self.vectorizer.idf_
        vocab = defaultdict(None, self.vectorizer.vocabulary_)
        vocab.default_factory = vocab.__len__
        inputs_tfidf = TfidfVectorizer()
        inputs_matrix = inputs_tfidf.fit_transform(inputs)
        # Add unseen vocab to existed corpus
        num_docs = inputs_matrix.shape[0]
        inputs_idf = inputs_tfidf.idf_
        inputs_vocab = inputs_tfidf.vocabulary_
        for word in inputs_vocab.keys():
            # Added with proper index if not in vocabulary
            if word not in vocab:
                vocab[word]
                df = (num_docs + 1) / np.exp(inputs_idf[inputs_vocab[word]] - 1) - 1
                df = np.log((self.tfidf.shape[0] + 1) / (df + 1)) + 1
                idf = np.append(idf, [df])
        # Construct workingset
        self.workingset_vec = TfidfVectorizer(vocabulary=vocab)
        self.workingset_vec.idf_ = idf
        # self.workingset_vec._idf_diag = sp.diags(idf, offsets=0,
        #                                 shape=(idf.shape[0], idf.shape[0]),
        #                                 format='csr',
        #                                 dtype=np.float64)
        self.workingset_tfidf = self.workingset_vec.transform(inputs)
    
    def _clear_workingset(self):
        self.idx = None
        self.workingset_vec = None
        self.workingset_tfidf = None
    
    def similar(self, text1, text2):
        if self.workingset_vec is None:
            inputs_tfidf = TfidfVectorizer()
            try:
                _ = inputs_tfidf.fit_transform([text1, text2])
            except: return 0
            self._init_workingset([text1, text2])
        idx1, idx2 = self.idx[text1], self.idx[text2]
        return cosine_similarity(self.workingset_tfidf[idx1].toarray(), self.workingset_tfidf[idx2].toarray())[0,0]
    
    def topN(self, text, N=10):
        if self.workingset_vec is None:
            inputs_tfidf = TfidfVectorizer()
            try:
                _ = inputs_tfidf.fit_transform([text])
            except: return ''
            self._init_workingset([text])
        array = self.workingset_tfidf[self.idx[text]].toarray()[0]
        idxes = array.argsort()[-N:]
        words = self.workingset_vec.get_feature_names()
        return [words[i] for i in reversed(idxes)]

    def add_corpus(self, inputs):
        if self.workingset_vec is not None:
            self._clear_workingset()
        inputs_tfidf = TfidfVectorizer()
        try:
            _ = inputs_tfidf.fit_transform(inputs)
        except: return
        self._init_workingset(inputs)


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
    soup = BeautifulSoup(html, 'lxml')
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


def goose_extract(html, lang=None):
    if not lang:
        g = Goose()
    else:
        g = Goose({'use_meta_language': False, 'target_language': lang})
    article = g.extract(raw_html=html)
    if article.cleaned_text == "":
        lang_code = detect_langs(html)[0].lang
        g = Goose({'use_meta_language': False, 'target_language': lang_code})
        article = g.extract(raw_html=html)
    return article.cleaned_text


def justext_extract(html, lang=None):
    lang_code = detect_langs(html)[0].lang if not lang else lang
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


def newspaper_extract(html, lang=None):
    lang_code = detect_langs(html)[0].lang if not lang else lang
    article = Article('https://google.com', language=lang_code) # Dummy urls to initialize the obj Can be anything able to wget
    article.download(input_html=html)
    article.parse()
    return article.text


def boilerpipe_extract(html, lang=None):
    extractor = Extractor(extractor="ArticleExtractor", html=html)
    return extractor.getText()


def domdistiller_extract(html, lang=None):
    """
    Insert domdistiller js into the html
    Filter out all src / href except for css
    Write Page into $PROJ_HOME/tmp with pid+ts
    Run chrome to load the page
    Call org.chromium.distiller to get the content 
    """
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup.find_all('', {'src': True}):
        del(tag.attrs['src'])
    for tag in soup.find_all('img', {'style': True}):
        del(tag.attrs['style'])
    filter_tags = ['script', 'link']
    for tag in filter_tags:
        for element in soup.find_all(tag):
            element.decompose()

    new_script = soup.new_tag('script')
    new_script.attrs.update({
        'src': "http://localhost:{}/domdistiller.js".format(config.LOCALSERVER_PORT),
        'type': 'text/javascript',
        'language': 'javascript'
    })
    if soup.head:
        soup.head.append(new_script)
    else:
        soup.insert(1, new_script)
    
    html_id = "{}_{}.html".format(time.time(), os.getpid())
    html_file = join(tmp_path, html_id)
    file = open(html_file, 'w+')
    file.write(str(soup))
    file.close()
    url = 'http://localhost:{}/{}'.format(config.LOCALSERVER_PORT, html_id)
    try:
        call(['node', join(dirname(abspath(__file__)), 'run_content.js'), url, '--filename', html_file, '--timeout', str(10)], timeout=20)
    except:
        print('DomDistiller Failed')
        os.remove(html_file)
        return ""
    content = open(html_file, 'r').read()
    os.remove(html_file)
    soup = BeautifulSoup(content, 'lxml')

    filter_tags = ['style', 'script', 'link', 'meta']
    for tag in filter_tags:
        for element in soup.findAll(tag):
            element.decompose()

    content = soup.get_text(separator=' ')
    filter_str = ['\n', ' ']
    for s in filter_str:
        string_list = content.split(s)
        string_list = list(filter(lambda x: x != s, string_list))
        content = s.join(string_list)
    return content


def lang_meta(html):
    """
    Grab the metadata of html
    """
    soup = BeautifulSoup(html, 'lxml')
    html = soup.find('html')
    try:
        return html['lang'][:2]
    except:
        return None


def extract_body(html, version='domdistiller', handle_exception=True):
    """
    Wrapper functions for different version of html body extraction
    """
    lang = lang_meta(html)
    backup_versions = ['domdistiller', 'boilerpipe', 'justext']
    backup_versions = [v for v in backup_versions if v != version]
    if html == '': return ''
    func_dict = {
        "justext": justext_extract,
        "goose": goose_extract,
        "newspaper": newspaper_extract,
        "boilerpipe": boilerpipe_extract,
        "domdistiller": domdistiller_extract
    }
    try:
        for v in [version] + backup_versions:
            content = func_dict[v](html, lang=lang)
            if content != "": return content
        return content
    except Exception as e:
        print("extract body:", str(e))
        if handle_exception: return ""
        else: raise


def newspaper_title_extract(html, lang=None):
    lang_code = detect_langs(html)[0].lang if not lang else lang
    article = Article('https://google.com', language=lang_code) # Dummy urls to initialize the obj Can be anything able to wget
    article.download(input_html=html)
    article.parse()
    return article.title


def mine_title_extract(html, lang=None):
    # TODO Inplement this func
    pass


def domdistiller_title_extract(html, lang=None):
    """
    Insert domdistiller js into the html
    Filter out all src / href except for css
    Write Page into $PROJ_HOME/tmp with pid+ts
    Run chrome to load the page
    Call org.chromium.distiller to get the title
    """
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup.find_all('', {'src': True}):
        del(tag.attrs['src'])
    for tag in soup.find_all('img', {'style': True}):
        del(tag.attrs['style'])
    filter_tags = ['script', 'link']
    for tag in filter_tags:
        for element in soup.find_all(tag):
            element.decompose()

    new_script = soup.new_tag('script')
    new_script.attrs.update({
        'src': "http://localhost:{}/domdistiller.js".format(config.LOCALSERVER_PORT),
        'type': 'text/javascript',
        'language': 'javascript'
    })
    if soup.head:
        soup.head.append(new_script)
    else:
        soup.insert(1, new_script)
    
    html_id = "{}_{}.html".format(time.time(), os.getpid())
    html_file = join(tmp_path, html_id)
    file = open(html_file, 'w+')
    file.write(str(soup))
    file.close()
    url = 'http://localhost:{}/{}'.format(config.LOCALSERVER_PORT, html_id)
    try:
        call(['node', join(dirname(abspath(__file__)), 'run_title.js'), url, '--filename', html_file, '--timeout', str(10)])
    except:
        print('DomDistiller Failed')
        os.remove(html_file)
        return ""
    title = open(html_file, 'r').read()
    os.remove(html_file)
    return title


def extract_title(html, version='mine'):
    """
    Wrapper functions for different version of html body extraction
    """
    lang = lang_meta(html)
    if html == '': return ''
    func_dict = {
        "newspaper": newspaper_title_extract,
        "mine": mine_title_extract,
        "domdistiller": domdistiller_title_extract
    }
    title = func_dict[version](html, lang=lang)
    if "Wayback Machine" in title: return ""
    return title


def k_shingling(text1, text2, k=5):
    text1 = text1.split()
    text2 = text2.split()
    if len(text1) < 5:
        shingle1 = [tuple(text1)]
    else:
        shingle1 = [tuple(text1[i: i+5]) for i in range(len(text1)-4)]
    if len(text2) < 5:
        shingle2 = [tuple(text2)]
    else:
        shingle2 = [tuple(text2[i: i+5]) for i in range(len(text2)-4)]
    if len(shingle1) + len(shingle2) <= 0: return 1
    return len(set(shingle1).intersection(set(shingle2))) / len(set(shingle1).union(set(shingle2)))

