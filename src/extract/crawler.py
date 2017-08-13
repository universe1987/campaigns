import os
import json
from collections import deque
from utils import campactify
from utils import is_valid_url
from utils import tokenize
from utils import to_camel
from html_to_json import html_to_json

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import JSON_DIR, ENTRY_DIR


def crawl(url):
    q = deque([url])
    processed = {url}
    domain = 'http://www.ourcampaigns.com/'

    while q:
        current_url = q.popleft()
        result = html_to_json(domain + current_url)
        if result is None:
            print '  skip', current_url
            continue
        category, uid = tokenize(current_url)

        if category == 'race':
            components = result['RACE DETAILS']['Parents'][0]['text'].split('>')
            if len(components) <= 2:
                print '  Bad', components, current_url
                continue
            if components[1].strip() != 'United States':
                continue
            position = campactify(components[-2] + components[-1])
            year = int(result['RACE DETAILS']['Term Start'][0]['text'].split('-')[0].split(',')[-1].strip())
            if year > 2016 or year < 1950:
                continue
            description = 'race_{}_{}'.format(position, year)
        elif category == 'candidate':
            name = campactify(result['CANDIDATE DETAILS']['Name'][0]['text'])
            description = 'candidate_{}'.format(name)
        elif category == 'container':
            name = campactify(result['INCUMBENT']['Name'][0]['text'])
            year = result['INCUMBENT']['Won'][0]['text'].split('/')[-1].strip()
            description = 'container_{}_{}'.format(name, year)

        for table_title, table in result.iteritems():
            camel_title = to_camel(table_title)
            if camel_title not in ['LastGeneralElection', 'PrimaryOtherSchedule']:
                with open(os.path.join(JSON_DIR, '{}_{}_{}.json'.format(description, uid, camel_title)), 'wb') as fp:
                    json.dump(table, fp)
            if category == 'race' and 'Governor' not in description and 'Mayor' not in description:
                continue
            for row_title, row in table.iteritems():
                for cell in row:
                    link = cell['link']
                    if link not in processed and is_valid_url(link):
                        q.append(link)
                        processed.add(link)


def crawl_state():
    with open(os.path.join(ENTRY_DIR, 'recent_elections_state.csv'), 'rb') as fp:
        data = fp.read().split('\n')
    for row in data:
        url, state = row.split(',')[:2]
        print 'Crawling', state
        crawl(url)


def crawl_city():
    with open(os.path.join(ENTRY_DIR, 'recent_elections_city.csv'), 'rb') as fp:
        data = fp.read().split('\n')
    for row in data:
        url, city, state = row.split(',')[:3]
        print 'Crawling', city, state
        crawl(url)


if __name__ == '__main__':
    crawl_city()
