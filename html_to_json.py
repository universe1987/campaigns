import re
import json
from bs4 import BeautifulSoup
from utils import clean_up, get_html, tokenize


def html_to_json(url):
    category, uid = tokenize(url)
    schema_name = 'schema/{}.json'.format(category)
    with open(schema_name, 'rb') as fp:
        template = json.load(fp)

    html_doc = get_html(url)
    soup = BeautifulSoup(html_doc, 'html.parser')

    table_title = None
    result = {}
    for tr in soup.find_all('tr'):
        is_title_row = False
        row_content = []
        for td in tr.find_all('td'):
            if td.find_all('img'):
                continue
            text = clean_up(td.text)
            if text in template:
                table_title = text
                is_title_row = True
                row_titles = template[table_title]
                result[table_title] = {}
                break
            link = ''
            for a in td.find_all('a'):
                link = a.get('href')
            row_content.append({'text': text, 'link': link})

        if is_title_row:
            continue

        if not row_content or not table_title:
            continue

        if isinstance(row_titles, list):
            first_cell_text = row_content[0]['text']
            if first_cell_text not in row_titles or first_cell_text in result[table_title]:
                continue
            result[table_title][first_cell_text] = row_content[1:]

        elif isinstance(row_titles, str) or isinstance(row_titles, unicode):
            if len(row_content) <= 1:
                table_title = None
                continue
            second_cell_text = row_content[1]['text']
            if not re.match(row_titles, second_cell_text):
                table_title = None
                continue
            category, race_id = tokenize(row_content[2]['link'])
            result[table_title][race_id] = row_content[1:]
    return result


if __name__ == '__main__':
    urls = ['http://www.ourcampaigns.com/RaceDetail.html?RaceID=613722',
            'http://www.ourcampaigns.com/CandidateDetail.html?CandidateID=234785',
            'http://www.ourcampaigns.com/ContainerDetail.html?ContainerID=131']

    for i, url in enumerate(urls):
        result = html_to_json(url)
        saveas = 'converted_{}.json'.format(i)
        with open(saveas, 'wb') as fp:
            json.dump(result, fp)
