"""
Implementation of detection of broken pages from sic transit 
"""
import requests
import re
import os
from urllib.parse import urlparse, parse_qsl
import random, string
import sys
from math import ceil

sys.path.append('../')
import config
from utils import text_utils
from utils.crawl import rp 
import logging
logger = logging.getLogger('logger')

def send_request(url):
    resp = None
    requests_header = {'user-agent': config.config('user_agent')}

    req_failed = True
    if not rp.allowed(url, requests_header['user-agent']):
        return None, 'Not Allowed'
    try:
        resp = requests.get(url, headers=requests_header, timeout=15)
        req_failed = False
    # Requsts timeout
    except requests.exceptions.ReadTimeout:
        error_msg = 'ReadTimeout'
    except requests.exceptions.Timeout:
        error_msg = 'Timeout'
    # DNS Error or tls certificate verify failed
    except requests.exceptions.ConnectionError as exc:
        reason = str(exc)
        # after looking for the failure info, the following should be the errno for DNS errors.
        if ("[Errno 11001] getaddrinfo failed" in reason or     # Windows
            "[Errno -2] Name or service not known" in reason or # Linux
            "[Errno 8] nodename nor servname " in reason):      # OS X
            error_msg = 'ConnectionError_DNSLookupError'
        else:
            error_msg = 'ConnectionError'
    except requests.exceptions.MissingSchema:
        error_msg = 'MissingSchema'
    except requests.exceptions.InvalidSchema:
        error_msg = 'InvalidSchema'
    except requests.exceptions.RequestException:
        error_msg = 'RequestException'
    except requests.exceptions.TooManyRedirects:
        error_msg = 'TooManyRedirects'
    except UnicodeError:
        error_msg = 'ERROR_UNICODE'
    except Exception as _:
        error_msg = 'ERROR_REQUEST_EXCEPTION_OCCURRED'

    if req_failed:
        return resp, error_msg

    return resp, 'SUCCESSFUL'


def get_status(url, resp, msg):
    status, detail = "", ""
    if msg == 'SUCCESSFUL':
        final_url, status_code = resp.url, resp.status_code
        url_path = urlparse(url).path
        final_url_path = urlparse(final_url).path
        # remove the last '/' if it exists
        if url_path.endswith('/'):
            url_path = url_path[:-1]
        if final_url_path.endswith('/'):
            final_url_path = final_url_path[:-1]
        
        status = str(status_code)
        # if the response status code is 400 or 500 level, brokem
        if int(status_code / 100) >= 4:
            detail = status_code
        # if status code is 200 level and no redirection
        elif (int(status_code/100) == 2 or int(status_code/100) == 3) and final_url_path == url_path:
            detail = 'no redirection'
        # if a non-hompage redirects to a homepage, considered broken
        elif final_url_path == '' and url_path != '':
            detail = 'homepage redirection'
        # if it redirects to another path, we are unsure.
        elif final_url_path != url_path:
            detail = 'non-home redirection'

        # do not know what redirection happens
        else:
            # this list should be empty
            detail = 'unknown redirection'
    else:
        if 'ConnectionError_DNSLookupError' in msg:
            status = 'DNSError'
        elif msg == 'TooManyRedirects':
            status = 'OtherError'
            detail = 'TooManyRedirects'
        else:
            status = 'OtherError'
            detail = 'othererror'
            if "DNS" in detail: status = "DNSError"
    return status, detail


def construct_rand_urls(url):
    """
    Construct random urls from given url. Randomed part satisfies:
        With same format. Consists of the same char format as old ones
        Return urls with all possible random construction
    """
    random_urls = []
    up = urlparse(url)
    def similar_pattern(name):
        sep = "$-_.+!*'(),"
        lower_char = [c for c in name if c.islower()]
        upper_char = [c for c in name if c.isupper()]
        num_char = [c for c in name if c.isdigit()]
        if (len(lower_char) + len(upper_char) + len(num_char)) == 0:
            return ''.join([random.choice(string.ascii_letters) for _ in range(25)])
        else: 
            ratio = ceil(25/(len(lower_char) + len(upper_char) + len(num_char)))
            for c in lower_char:
                name = name.replace(c, ''.join([random.choice(string.ascii_lowercase) for _ in range(ratio)]))
            for c in upper_char:
                name = name.replace(c, ''.join([random.choice(string.ascii_uppercase) for _ in range(ratio)]))
            for c in num_char:
                name = name.replace(c, ''.join([random.choice(string.digits) for _ in range(ratio)]))
            return name
    scheme, netloc, path, query = up.scheme, up.netloc, up.path, up.query
    end_with_slash = False
    if path == '': path += '/'
    elif path != '/' and path[-1] == '/': 
        end_with_slash = True
        path = path[:-1]
    # Filename Random construction
    url_dir, filename = os.path.dirname(path), os.path.basename(path)
    random_filename = similar_pattern(filename)
    random_url = f"{scheme}://{netloc}{os.path.join(url_dir, random_filename)}"
    if end_with_slash: random_url += '/'
    if query: random_url += '?' + query
    random_urls.append(random_url)
    # Query Random construct
    if not query: return random_urls
    ql = parse_qsl(query)
    if len(ql) == 0: # Not valid query string. Replace all together
        q = similar_pattern(query)
        random_url = f"{scheme}://{netloc}{path}"
        if end_with_slash: random_url += '/'
        random_url += '?' + q
        random_urls.append(random_url)
    else:
        for idx, qkv in enumerate(ql):
            qv = similar_pattern(qkv[1])
            query_cp = ql.copy()
            query_cp[idx] = (qkv[0], qv)
            rand_query = '&'.join([f'{q[0]}={q[1]}' for q in query_cp])
            random_url = f"{scheme}://{netloc}{path}"
            if end_with_slash: random_url += '/'
            random_url += '?' + rand_query
            random_urls.append(random_url)
    return random_urls


def filter_redir(r):
    """Filter out simple redirections from http --> https"""
    old_his = [h.url for h in r.history] + [r.url]
    new_his = []
    for idx, (h_bef, h_aft) in enumerate(zip(old_his[:-1], old_his[1:])):
        if not h_bef.split('://')[-1] == h_aft.split('://')[-1]:
            new_his.append(r.history[idx])
    return new_his


def broken(url, html=False):
    """
    Entry func: detect whether this url is broken
    html: Require the url to be html.

    Return: True/False/"N/A", reason
    """
    resp, msg = send_request(url)
    if msg == 'Not Allowed':
        return 'N/A', msg
    status, _ = get_status(url, resp, msg)
    if re.compile('^([45]|DNSError|OtherError)').match(status):
        return True, status
    headers = {k.lower(): v for k, v in resp.headers.items()}
    content_type = headers['content-type'] if 'content-type' in headers else ''
    if html and 'html' not in content_type:
        logger.info('sic transit broken: Not HTML')
        return True, "Not html"
    # Construct new url with random filename
    random_urls = construct_rand_urls(url)
    broken_decision, reasons = [], []
    for random_url in random_urls:
        random_resp, msg = send_request(random_url)
        if msg == 'Not Allowed':
            continue
        random_status, _ = get_status(random_url, random_resp, msg)
        if re.compile('^([45]|DNSError|OtherError)').match(random_status):
            broken_decision.append(False)
            reasons.append("random url hard broken")
            continue
        # Filter out http --> https redirection
        if len(filter_redir(resp)) != len(filter_redir(random_resp)):
            broken_decision.append(False)
            reasons.append("#redirection doesn't match")
            continue
        if resp.url == random_resp.url:
            broken_decision.append(True)
            reasons.append("Same final url")
            continue
        # url_content = text_utils.extract_body(resp.text, version='domdistiller')
        # random_content = text_utils.extract_body(random_resp.text, version='domdistiller')
        if text_utils.k_shingling(resp.text, random_resp.text) >= 0.95:
            broken_decision.append(True)
            reasons.append("Similar soft 404 content")
            continue
        broken_decision.append(False)
        reasons.append("no features match")
    if len(reasons) == 0:
        return 'N/A', 'Guess URLs not allowed'
    else:
        return not False in broken_decision, reasons
    


