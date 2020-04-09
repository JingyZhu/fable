"""
Implementation of detection of broken pages from sic transit 
"""
import requests
import re
import os
from urllib.parse import urlparse
import random, string
import sys

sys.path.append('../')
import config
from utils import text_utils

def send_request(url):
    resp = None
    requests_header = {'user-agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"}

    req_failed = True
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


def broken(url):
    """
    Entry func: detect whether this url is broken
    Return: bool (broken), reason
    """
    resp, msg = send_request(url)
    status, _ = get_status(url, resp, msg)
    if re.compile('^([45]|DNSError|OtherError)').match(status):
        return True, status
    path = urlparse(url).path
    if path == '': url += '/'
    elif path != '/' and url[-1] == '/': url = url[:-1]
    url_dir = os.path.dirname(url)
    random_filename = ''.join([random.choice(string.ascii_letters) for _ in range(25)])
    random_url = url_dir + '/' + random_filename
    random_resp, msg = send_request(random_url)
    random_status, _ = get_status(random_url, random_resp, msg)
    if re.compile('^([45]|DNSError|OtherError)').match(random_status):
        return False, "random url hard broken"
    if len(resp.history) != len(random_resp.history):
        return False, "#redirection doesn't match"
    if resp.url == random_resp.url:
        return True, "Same final url"
    url_content = text_utils.extract_body(resp.text, version='domdistiller')
    random_content = text_utils.extract_body(random_resp.text, version='domdistiller')
    if text_utils.k_shingling(url_content, random_content) > 0.8:
        return True, "Similar soft 404 content"
    return False, "no features match"
    


