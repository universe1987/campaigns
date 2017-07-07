import os
import requests


def clean_up(s):
    ascii_part = [c for c in s if ord(c) < 128]
    x = ''.join(ascii_part).strip()
    return ' '.join(x.split())


def tokenize(url):
    info = url.split('?')[-1].split('=')
    category = info[0][:-2].lower()
    uid = info[1]
    return category, uid


def get_html(url):
    category, uid = tokenize(url)
    name = '{}_{}'.format(category, uid)
    local_cache = 'cache/{}.html'.format(name)
    if not os.path.exists(local_cache):
        resp = requests.get(url)
        if resp.status_code == 200:
            html_doc = resp.content
            with open(local_cache, 'wb') as fp:
                fp.write(html_doc)
            return html_doc
    else:
        with open(local_cache, 'rb') as fp:
            return fp.read()


def is_valid_url(url):
    kws = ['RaceID', 'CandidateID', 'ContainerID']
    for kw in kws:
        if kw in url:
            return True
    return False
