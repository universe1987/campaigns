import os
import string
import requests
from time import sleep

valid = set(string.letters + '-')


def clean_up(s):
    ascii_part = [c for c in s if ord(c) < 128]
    x = ''.join(ascii_part).strip()
    return ' '.join(x.split())


def tokenize(url):
    info = url.split('?')[-1].split('=')
    category = info[0][:-2].lower()
    uid = info[1]
    return category, uid


def get_html(url, max_retry=5):
    category, uid = tokenize(url)
    name = '{}_{}'.format(category, uid)
    local_cache = 'cache/{}.html'.format(name)
    if not os.path.exists(local_cache):
        retry_count = 0
        while True:
            retry_count += 1
            if retry_count > max_retry:
                return None
            try:
                resp = requests.get(url + '&ShowAllCand=Y')
            except Exception as e:
                print e
                continue
            if resp.status_code == 200:
                html_doc = resp.content
                with open(local_cache, 'wb') as fp:
                    fp.write(html_doc)
                return html_doc
            sleep(3)
    else:
        with open(local_cache, 'rb') as fp:
            return fp.read()


def is_valid_url(url):
    if '=' not in url:
        return False
    category, uid = tokenize(url)
    return category in ['race', 'candidate']


def campactify(s):
    return ''.join([x for x in s if x in valid])


def to_camel(s):
    return ''.join([t.capitalize() for t in s.replace('/', ' ').split()])
