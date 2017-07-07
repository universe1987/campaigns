import os
import json
import csv
from utils import tokenize
from utils import is_valid_url
from html_to_json import html_to_json


def crawl(prefix, url, processed):
    print url
    processed.add(url)
    # print url
    domain = 'http://www.ourcampaigns.com/'
    result = html_to_json(domain + url)
    category, uid = tokenize(url)
    for table_title, table in result.iteritems():
        camel_title = ''.join([s.capitalize() for s in table_title.replace('/', ' ').split()])
        with open('{}/{}_{}_{}.json'.format(prefix, category, uid, camel_title), 'wb') as fp:
            json.dump(table, fp)
        for row in table.itervalues():
            for cell in row:
                link = cell['link']
                if is_valid_url(link) and link not in processed:
                    crawl(prefix, link, processed)


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
            print row
            container_id, state, year = row[:3]
            folder_name = 'data/' + state
            if not os.path.exists(folder_name):
                os.mkdir(folder_name)
            url = url_template.format(container_id)
            crawl(folder_name, url, set())
