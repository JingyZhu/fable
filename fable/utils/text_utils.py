"""
Utils for text
"""
import justext
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from langcodes import Language
from langdetect import detect_langs
from goose3 import Goose
from newspaper import Article
from boilerpipe.extract import Extractor
import brotli
import bs4
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
import functools

from . import url_utils
try:
    from fable import config
except:
    print("No config.py, Specify you own port")

sys.setrecursionlimit(1500)
# Need to be modified
tmp_path = config.TMP_PATH
port = config.LOCALSERVER_PORT
vectorizer_kwargs = {'stop_words': 'english'}

NULL = open('/dev/null', 'w')

def localserver(PORT):
    """
    Create tmp dir at $PROJ_HOME, copy domdistiller.js into the repo
    Serve a local server at port if it not occupied by any others
    """
    cur_path = os.path.dirname(__file__)
    call(['mkdir', '-p', tmp_path])
    if not os.path.exists(os.path.join(tmp_path, 'utils', 'domdistiller.js')):
        call(['cp', os.path.join(cur_path, 'domdistiller.js'), tmp_path])
    port_occupied = re.compile(":{}".format(port)).findall(check_output(['netstat', '-nlt']).decode())
    if len(port_occupied) > 0:
        # * Try kill http-server once 
        call(['pkill', 'http-server'])
    port_occupied = re.compile(":{}".format(port)).findall(check_output(['netstat', '-nlt']).decode())
    if len(port_occupied) <= 0:
        Popen(['http-server', '-a', 'localhost', '-p', str(port), tmp_path], stdout=NULL, stderr=NULL)
    else:
        # * Port is not occupied by http-server 
        print(f"Port {port} occupied by other process", file=sys.stderr)

localserver(port)

from contextlib import contextmanager
import threading
import _thread

class TimeoutException(Exception):
    def __init__(self, msg=''):
        self.msg = msg

@contextmanager
def time_limit(seconds, msg=''):
    timer = threading.Timer(seconds, lambda: _thread.interrupt_main())
    timer.start()
    try:
        yield
    except KeyboardInterrupt:
        raise TimeoutException("Timed out for operation {}".format(msg))
    finally:
        # if the action ends in specified time, timer is canceled
        timer.cancel()

class TFidfDynamic:
    def re_init(self):
        """
        Re calculated the tfidf from the self.corpus
        """
        print("re_init")
        # self.vectorizer = TfidfVectorizer()
        self.tfidf = self.vectorizer.fit_transform(self.corpus)
        self.pairwise_simi = None
        # print(f"Takes {(time.time()-begin):.2f}s")

    def __init__(self, corpus):
        corpus = list(set(corpus))
        self.idx = {c: i for i, c in enumerate(corpus)}
        self.corpus = corpus
        self.vectorizer = TfidfVectorizer(**vectorizer_kwargs)
        self.tfidf = self.vectorizer.fit_transform(corpus)
        self.pairwise_simi = None

    def similar(self, text1, text2):
        """
        Get similarity of 2 text
        If any of the text is not in the corpus, TFIDF matrix will be recalculated
        """
        idx1, idx2 = self.idx[text1], self.idx[text2]
        return cosine_similarity(self.tfidf[idx1], self.tfidf[idx2])[0,0]
    
    def topN(self, text, N=7):
        """
        Get the highest weighted N words in a text
        If text is not in the corpus, it'll be added, and tfidf'll be recalculated
        """
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
    
    def _gen_pair_simi(self):
        """Generate all pairwise documents' similarity"""
        if self.pairwise_simi is not None: return
        self.pairwise_simi = cosine_similarity(self.tfidf)

    def top_similar(self, text, N=10):
        """
        N: Number of documents returned
        Return: [(document, similarity)] for top similar documents to text.
        """
        self._gen_pair_simi()
        idx = self.idx[text]
        array = self.pairwise_simi[idx]
        idxes = array.argsort()[-N-1:]
        return [(self.corpus[i], array[i]) for i in reversed(idxes) if i != idx]

class TFidfStatic:
    def __init__(self, corpus):
        corpus = list(set(corpus))
        self.corpus = corpus
        self.vectorizer = TfidfVectorizer(**vectorizer_kwargs)
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
        # vocab = defaultdict(None, self.vectorizer.vocabulary_)
        vocab = self.vectorizer.vocabulary_.copy()
        vsize = len(vocab)
        # vocab.default_factory = vocab.__len__
        inputs_tfidf = TfidfVectorizer(**vectorizer_kwargs)
        inputs_matrix = inputs_tfidf.fit_transform(inputs)
        # Add unseen vocab to existed corpus
        num_docs = inputs_matrix.shape[0]
        inputs_idf = inputs_tfidf.idf_
        inputs_vocab = inputs_tfidf.vocabulary_
        for word in inputs_vocab.keys():
            # Added with proper index if not in vocabulary
            if word not in vocab:
                vocab[word] = vsize
                vsize += 1
                df = (num_docs + 1) / np.exp(inputs_idf[inputs_vocab[word]] - 1) - 1
                df = np.log((self.tfidf.shape[0] + 1) / (df + 1)) + 1
                idf = np.append(idf, [df])
        # Construct workingset
        self.workingset_vec = TfidfVectorizer(vocabulary=vocab, **vectorizer_kwargs)
        def my_validate_vocab(self):
            self.vocabulary_ = self.vocabulary
        self.workingset_vec._validate_vocabulary = functools.partial(my_validate_vocab, self.workingset_vec)
        self.fixed_vocabulary_ = True       
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
            inputs_tfidf = TfidfVectorizer(**vectorizer_kwargs)
            try:
                _ = inputs_tfidf.fit_transform([text1, text2])
            except: return 0
            self._init_workingset([text1, text2])
        idx1, idx2 = self.idx[text1], self.idx[text2]
        return cosine_similarity(self.workingset_tfidf[idx1], self.workingset_tfidf[idx2])[0,0]
    
    def topN(self, text, N=7):
        if self.workingset_vec is None:
            inputs_tfidf = TfidfVectorizer(**vectorizer_kwargs)
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
        inputs_tfidf = TfidfVectorizer(**vectorizer_kwargs)
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


def tokenize(texts):
    """
    Simple function for tokenizing a text. Extracted from sklearn src code
    
    Returns: list of features in the original order
    """
    texts = texts.replace('_', ' ')
    cv = CountVectorizer(**vectorizer_kwargs) # TODO: Not necessary english
    analyze = cv.build_analyzer()
    return analyze(texts)


def article_date(html, url=""):
    """
    Get the publish date of a webpage using article library
    Return datetime.datetime
    """
    article = Article(url=url)
    article.download(input_html=html)
    article.parse()
    return article.publish_date


def mine_date(html, url=""):
    """
    Mine way of trying to get date
    """
    soup = BeautifulSoup(html, 'lxml')
    wm_ipp = soup.find_all('div', id='wm-ipp-base')
    if len(wm_ipp) > 0: wm_ipp[0].decompose()
    donato = soup.find_all('div', id='donato')
    if len(donato) > 0: donato[0].decompose()
    dates = set()
    filter_tags = ['script', 'a', 'style']
    for tag in filter_tags:
        for certain_tag in soup.findAll(tag):
            certain_tag.decompose()
    tag_list = ['div', 'p', 'span', 'b'] + ['h{}'.format(i) for i in range(1, 4)]
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
            # dt = dt.strftime("%Y %m %d")
            dates.add((dt, len(piece.text.split())))
    for time_tag in soup.find_all('time'):
        if 'datetime' in time_tag.attrs:
            dt = time_tag.attrs['datetime']
            dt = dparser.parse(dt, fuzzy=True)
            dates.add((dt, 0))
    dates = sorted([o for o in dates], key=lambda x: x[1])
    # print(dates)
    return dates[0][0] if len(dates) > 0 else None


def extract_date(html, version="article", url=""):
    """
    Wrapper function for different version of date extraction
    """
    backup_versions = ['article', 'mine']
    backup_versions = [v for v in backup_versions if v != version]
    if html == '': return ''
    func_dict = {
        "mine": mine_date,
        "article": article_date
    }
    for v in [version] + backup_versions:
        try:
            with time_limit(10):
                date = func_dict[v](html, url=url)
                print(v, date)
                if date: return date
        except:
            continue
    return date

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
    article = Article(url='', language=lang_code) # Dummy urls to initialize the obj Can be anything able to wget
    article.download(input_html=html)
    article.parse()
    return article.text


def boilerpipe_extract(html, lang=None):
    extractor = Extractor(extractor="ArticleExtractor", html=html)
    text = extractor.getText()
    if not isinstance(text, str):
        text = str(text)
    return text

def unwrap_tags(soup):
    has_content = lambda x: x and len(re.sub('[ \n]', '', x)) > 0
    
    for tag in soup.findAll():
        if (isinstance(tag.previous_sibling, bs4.element.NavigableString) and has_content(tag.previous_sibling)) \
           or (isinstance(tag.next_sibling, bs4.element.NavigableString) and has_content(tag.next_sibling)):
            tag.unwrap()
    for tag in soup.findAll():
        allNavi = True
        for element in tag.contents:
            if not isinstance(element, bs4.element.NavigableString):
                allNavi = False
                break
        if allNavi:
            tag.string = ' '.join(tag.contents)
    return soup

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
    try:
        file.write(str(soup))
        file.close()
    except:
        return ''
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

    soup = unwrap_tags(soup)
    try:
        content = soup.get_text(separator='||', strip=True)
    except:
        return ''
    # print(soup, content)
    filter_str = ['\n']
    for s in filter_str:
        string_list = content.split(s)
        string_list = list(filter(lambda x: x != s, string_list))
        content = ' '.join(string_list)
    string_list = content.split('||')
    string_list = list(filter(lambda x: x != '||' and x.replace(' ', ''), string_list))
    content = '\n'.join(string_list)
    if content == '':
        print("Domdistiller empty")
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
    if version is list, no backup applied
    """
    lang = lang_meta(html)
    backup_versions = ['domdistiller', 'boilerpipe']
    if isinstance(version, str):
        backup_versions = [v for v in backup_versions if v != version]
    elif isinstance(version, list):
        version = version[0]
        backup_versions = []
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
        raise


def newspaper_title_extract(html, lang=None):
    lang_code = detect_langs(html)[0].lang if not lang else lang
    article = Article('https://google.com', language=lang_code) # Dummy urls to initialize the obj Can be anything able to wget
    article.download(input_html=html)
    article.parse()
    return article.title


def mine_title_extract(html, lang=None):
    # TODO Inplement this func
    soup = BeautifulSoup(html, 'lxml')
    if not soup.title:
        return "" # ? Really should be None
    return soup.title.text


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
    try:
        file.write(str(soup))
        file.close()
    except:
        return ''
    url = 'http://localhost:{}/{}'.format(config.LOCALSERVER_PORT, html_id)
    try:
        call(['node', join(dirname(abspath(__file__)), 'run_title.js'), url, '--filename', html_file, '--timeout', str(10)], timeout=20)
    except:
        print('DomDistiller Failed')
        os.remove(html_file)
        return ""
    title = open(html_file, 'r').read()
    os.remove(html_file)
    return title


def boilerpipe_title_extract(html, lang=None):
    extractor = Extractor(extractor="ArticleExtractor", html=html)
    text = extractor.source.getTitle()
    if not isinstance(text, str):
        text = str(text)
    return text


def extract_title(html, version='mine', handle_exception=True):
    """
    Wrapper functions for different version of html title extraction
    """
    lang = lang_meta(html)
    if html == '': return ''
    backup_versions = ['domdistiller', 'newspaper']
    backup_versions = [v for v in backup_versions if v != version]
    func_dict = {
        "newspaper": newspaper_title_extract,
        "mine": mine_title_extract,
        "domdistiller": domdistiller_title_extract,
        "boilerpipe": boilerpipe_title_extract
    }
    title = func_dict[version](html, lang=lang)
    try:
        for v in [version] + backup_versions:
            title = func_dict[v](html, lang=lang)
            if title is not None: return title if "Wayback Machine" not in title else ""
            else:
                continue
    except Exception as e:
        print("extract body:", str(e))
        if handle_exception: return ""
        else: raise


def domdistiller_title_body_extract(html, lang=None):
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
    try:
        file.write(str(soup))
        file.close()
    except:
        return '', ''
    url = 'http://localhost:{}/{}'.format(config.LOCALSERVER_PORT, html_id)
    try:
        call(['node', join(dirname(abspath(__file__)), 'run_title_content.js'), url, '--filename', html_file, '--timeout', str(10)], timeout=20)
    except:
        print('DomDistiller Failed')
        os.remove(html_file)
        return "", ""
    title_content = open(html_file, 'r').read()
    title_content = title_content.split('\n')
    title, content = title_content[0], '\n'.join(title_content[1:])
    # * Process title
    if "Wayback Machine" in title: title = ""
    # * Process content
    soup = BeautifulSoup(content, 'lxml')

    filter_tags = ['style', 'script', 'link', 'meta']
    for tag in filter_tags:
        for element in soup.findAll(tag):
            element.decompose()

    soup = unwrap_tags(soup)
    try:
        content = soup.get_text(separator='||', strip=True)
    except:
        return '', ''
    # print(soup, content)
    filter_str = ['\n']
    for s in filter_str:
        string_list = content.split(s)
        string_list = list(filter(lambda x: x != s, string_list))
        content = ' '.join(string_list)
    string_list = content.split('||')
    string_list = list(filter(lambda x: x != '||' and x.replace(' ', ''), string_list))
    content = '\n'.join(string_list)
    os.remove(html_file)
    return title, content


def extract_title_body(html, handle_exception=True):
    """
    Wrapper functions for different version of html title & body extraction
    """
    lang = lang_meta(html)
    if html == '': return '', ''
    title, content = domdistiller_title_body_extract(html, lang=lang)
    if content != "":
        return title, content
    else:
        if title == "":
            title = extract_title(html)
        content = extract_body(html, version=['boilerpipe'])
        return title, content


def k_shingling(text1, text2, k=5):
    text1 = tokenize(text1)
    text2 = tokenize(text2)
    if len(text1) < k:
        shingle1 = [tuple(text1)]
    else:
        shingle1 = [tuple(text1[i: i+k]) for i in range(len(text1)-(k-1))]
    if len(text2) < k:
        shingle2 = [tuple(text2)]
    else:
        shingle2 = [tuple(text2[i: i+k]) for i in range(len(text2)-(k-1))]
    if len(shingle1) + len(shingle2) <= 0: return 1
    return len(set(shingle1).intersection(set(shingle2))) / len(set(shingle1).union(set(shingle2)))

