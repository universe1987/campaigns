import os
import json
import csv
from collections import deque
from utils import tokenize
from utils import is_valid_url
from html_to_json import html_to_json


def crawl(prefix, url, processed):
    q = deque([url])
    processed.add(url)
    domain = 'http://www.ourcampaigns.com/'
    while q:
        current_url = q.popleft()
        result = html_to_json(domain + current_url)
        category, uid = tokenize(current_url)
        for table_title, table in result.iteritems():
            camel_title = ''.join([s.capitalize() for s in table_title.replace('/', ' ').split()])
            if camel_title not in ['LastGeneralElection', 'PrimaryOtherSchedule']:
                with open('{}/{}_{}_{}.json'.format(prefix, category, uid, camel_title), 'wb') as fp:
                    json.dump(table, fp)
            for row in table.itervalues():
                for cell in row:
                    link = cell['link']
                    if is_valid_url(link) and link not in processed:
                        q.append(link)
                        processed.add(link)


if __name__ == '__main__':
    # create a folder for cache
    if not os.path.exists('cache'):
        os.mkdir('cache')
    # create a folder for extracted data
    if not os.path.exists('data'):
        os.mkdir('data')
    url_template = 'ContainerDetail.html?ContainerID={}'
    with open('governor.csv', 'rb') as fp:
        reader = csv.reader(fp, delimiter=',')
        for row in reader:
            container_id, state, year = row[:3]
            print 'Crawling', state
            folder_name = 'data/' + state
            if not os.path.exists(folder_name):
                os.mkdir(folder_name)
            url = url_template.format(container_id)
            crawl(folder_name, url, set())
