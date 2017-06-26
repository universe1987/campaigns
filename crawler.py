import os
import json
import string
import requests
from hashlib import sha1
from bs4 import BeautifulSoup


def keep_ascii(s):
    ascii_part = [c for c in s if ord(c) < 128]
    return ''.join(ascii_part).strip()


def get_html(url):
    name = sha1(url).hexdigest()
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


def get_info_tables(html_doc):
    soup = BeautifulSoup(html_doc, 'html.parser')
    info_tables = soup.find_all('table', {'class': 'infotable'})
    tables = []
    for tb in info_tables:
        table = []
        for tr in tb.find_all('tr'):
            row = []
            for td in tr.find_all('td'):
                # filter out images
                if td.find('img') is not None:
                    continue
                td_class = td.get('class')
                link = ''
                # only keep the last link
                for a in td.find_all('a'):
                    link = a.get('href')
                    if td_class is None:
                        td_class = a.get('class')
                # filter out positioning cells
                if td_class:
                    text = keep_ascii(td.text)
                    if text and 'javascript' not in text and 'google.maps' not in text:
                        row.append([text, link])
            table.append(row)
        if len(table) > 0 and len(table[0]) > 0:
            tables.append(table)
    return tables


def get_candidate_tables(html_doc):
    soup = BeautifulSoup(html_doc, 'html.parser')
    table_rows = soup.find_all('tr')
    content = []
    for tr_idx, tr in enumerate(table_rows):
        if tr_idx <= 4:
            continue
        row = []
        for td in tr.find_all('td'):
            link = ''
            for a in td.find_all('a'):
                link = a.get('href')
            text = keep_ascii(td.text)
            if text and 'javascript' not in text + link:
                row.append([text, link])
        if row:
            content.append(row)
    return content


def get_tables(html_doc, job_type):
    if job_type.startswith('Candidate'):
        return get_candidate_tables(html_doc)
    else:
        return get_info_tables(html_doc)


def extract_term_start(table):
    for row in table:
        for i in range(len(row)):
            if row[i][0] == 'Term Start':
                year = row[i + 1][0].split(',')[1].split('-')[0].strip()
                year = int(year)
                return year


def extract_year_and_link(table):
    for row in table:
        for i in range(len(row)):
            if row[i][0] == 'Won':
                year = row[i + 1][0].split('/')[-1]
                year = int(year)
                link = row[i + 1][1]
                return year, link


def get_next_jobs(tables, job_type):
    next_jobs = []
    url_head = 'http://www.ourcampaigns.com/'

    # if job_type is 'Incumbent' or 'General', we need to crawl links on that page
    if job_type == 'Incumbent':
        for t in tables:
            title = t[0][0][0]
            if title == 'INCUMBENT':
                previous_year, link = extract_year_and_link(t)
                if previous_year >= 1950:
                    next_jobs.append([url_head + link, 'General', previous_year])
                break

    elif job_type == 'General':
        for t in tables:
            title = t[0][0][0]
            if title == 'LAST GENERAL ELECTION':
                previous_year, link = extract_year_and_link(t)
                if previous_year >= 1950:
                    next_jobs.append([url_head + link, 'General', previous_year])
            elif title == 'RACE DETAILS':
                current_year = extract_term_start(t)
            elif title == 'PRIMARY/OTHER SCHEDULE':
                for row in t:
                    if len(row) <= 1:
                        continue
                    if 'Primary' in row[1][0]:
                        which_primary = row[1][0].split('-')[-1].strip().replace(' ', '_')
                        link = row[1][1]
                        year = int(row[0][0].split(',')[-1].strip())
                        next_jobs.append([url_head + link, which_primary, year])
            elif title == 'CANDIDATES':
                for row in t:
                    if row[0][0] == 'Name':
                        for candidate in row[1:]:
                            name, link = candidate
                            simplified = ''.join([c for c in name if c.lower() in string.ascii_lowercase + ' '])
                            name = simplified.replace(' ', '_')
                            if link:
                                next_jobs.append([url_head + link, 'Candidate_' + name, current_year])

    elif 'Primary' in job_type:
        for t in tables:
            title = t[0][0][0]
            if title == 'RACE DETAILS':
                current_year = extract_term_start(t)

            elif title == 'CANDIDATES':
                for row in t:
                    if row[0][0] == 'Name':
                        for candidate in row[1:]:
                            name, link = candidate
                            simplified = ''.join([c for c in name if c.lower() in string.ascii_lowercase + ' '])
                            name = simplified.replace(' ', '_')
                            if link:
                                next_jobs.append([url_head + link, 'Candidate_' + name, current_year])

    return next_jobs


def crawl(url, state, year, job_type, cache):
    key = (url, state, year, job_type)
    if key in cache:
        return
    print url
    html_doc = get_html(url)
    if html_doc is not None:
        tables = get_tables(html_doc, job_type)
        next_jobs = get_next_jobs(tables, job_type)
        store_name = 'data/{}_{}_{}.json'.format(state, year, job_type)
        with open(store_name, 'wb') as fp:
            print store_name
            json.dump(tables, fp)
        cache.add(key)
        for job in next_jobs:
            next_url, next_job_type, year = job
            crawl(next_url, state, year, job_type=next_job_type, cache=cache)


if __name__ == '__main__':
    # create a folder for cache
    if not os.path.exists('cache'):
        os.mkdir('cache')
    # create a folder for extracted data
    if not os.path.exists('data'):
        os.mkdir('data')
    url_template = 'http://www.ourcampaigns.com/ContainerDetail.html?ContainerID={}'
    container_id = 131
    url = url_template.format(container_id)
    crawl(url, 'Alaska', 2014, 'Incumbent', set())
