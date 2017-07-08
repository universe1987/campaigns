import os
import json
import csv
from collections import deque
from utils import to_camel
from utils import campactify
from utils import tokenize
from utils import is_valid_url
from html_to_json import html_to_json


def crawl(url):
    q = deque([url])
    with open('processed.txt', 'rb') as fp:
        processed = set(fp.read().split())
    processed.add(url)
    domain = 'http://www.ourcampaigns.com/'
    while q:
        current_url = q.popleft()
        if current_url.startswith(domain):
            current_url = current_url[len(domain):]
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
        # print '    ' + description, current_url
        for table_title, table in result.iteritems():
            camel_title = to_camel(table_title)
            if camel_title not in ['LastGeneralElection', 'PrimaryOtherSchedule']:
                with open('data/{}_{}_{}.json'.format(description, uid, camel_title), 'wb') as fp:
                    json.dump(table, fp)
            if category == 'race' and 'Governor' not in description:
                continue
            for row_title, row in table.iteritems():
                for cell in row:
                    link = cell['link']
                    if is_valid_url(link) and link not in processed:
                        q.append(link)
                        processed.add(link)
    with open('processed.txt', 'wb') as fp:
        fp.write('\n'.join(processed))


if __name__ == '__main__':
    # create a folder for cache
    if not os.path.exists('cache'):
        os.mkdir('cache')
    # create a folder for extracted data
    if not os.path.exists('data'):
        os.mkdir('data')
    with open('processed.txt', 'wb') as fp:
        pass
    url_template = 'ContainerDetail.html?ContainerID={}'
    with open('governor.csv', 'rb') as fp:
        reader = csv.reader(fp, delimiter=',')
        for row in reader:
            container_id, state, year = row[:3]
            print 'Crawling', state
            url = url_template.format(container_id)
            crawl(url)
