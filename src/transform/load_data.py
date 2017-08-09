import os
import re
import sys
import json
import pandas as pd
from glob import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.keep_ascii import keep_ascii


# Merge Race Details
def select_tables(folder, office, table_title):
    content = {'RaceID': []}
    input_file_pattern = '{}/race*{}*{}.json'.format(folder, office, table_title)
    n_row = 0
    header = set()
    for filename in glob(input_file_pattern):
        race_id = re.findall('\d+', filename)[1]
        with open(filename, 'rb') as fp:
            table = json.load(fp)
            new_row_sizes = []
            processed_key = set()
            for key, values in table.iteritems():
                if "ertified votes" in key.lower():
                    key = "Votes"
                key_text = key
                if key_text not in header:
                    header.add(key_text)
                    content[key_text] = [''] * n_row
                content[key_text].extend([keep_ascii(v['text']) for v in values])
                key_link = key + ' link'
                if key_link not in header:
                    header.add(key_link)
                    content[key_link] = [''] * n_row
                content[key_link].extend([v['link'] for v in values])
                new_row_sizes.append(len(values))
                processed_key.add(key_text)
                processed_key.add(key_link)
            if new_row_sizes:
                assert min(new_row_sizes) == max(new_row_sizes)
                n_new_row = new_row_sizes[0]
                n_row += n_new_row
                content['RaceID'] += [race_id] * n_new_row
                for missing_key in header - processed_key:
                    content[missing_key] += [''] * n_new_row

    header.add(u'RaceID')
    header = sorted(list(header))
    data = [[content[h][i] for h in header] for i in range(n_row)]

    df = pd.DataFrame(data, columns=header)
    return df


def generate_race_details_table2():
    df = select_tables('../../data/json', 'Governor', 'RaceDetails')
    date_cols = ['Polls Close', 'Term Start', 'Term End']
    for column_title in date_cols:
        df[column_title] = pd.to_datetime(df[column_title].str.split('-').str[0], errors='coerce')
    df['State'] = df['Parents'].str.split('>').str[2].str.strip()
    lookup = {"Hawai'i": "Hawaii", "Territory of Alaska": "Alaska", "Territory of Hawai'i": "Hawaii"}
    for i in xrange(len(df)):
        k = df['State'][i]
        if k in lookup:
            df.loc[i, 'State'] = lookup[k]
    df['Position'] = df['Parents'].str.split('>').str[3].str.strip()
    df['Term Length'] = df['Term End'] - df['Term Start']
    df['ContributorID'] = df['Contributor link'].str.extract('(\d+)', expand=False)
    df['Data Sources'] = df['Data Sources'].str.replace('\[Link\]', "")
    df['Turnout'] = df['Turnout'].str.extract('(\d+\.?\d*)% Total Population', expand=False).astype(float)
    to_drop = ['Contributor link', 'Filing Deadline', 'Filing Deadline link', 'Last Modified', 'Last Modified link',
               'Office link', 'Parents link', 'Polls Close link', 'Polls Open', 'Polls Open link', 'Term End link',
               'Term Start link', 'Turnout link', 'Type link', 'Data Sources link']
    df.drop(to_drop, axis=1, inplace=True)
    return df


def generate_candidate_table():
    df = select_tables('../../data/json', 'Governor', 'Candidates')
    df = df[df['Name'] != '']
    df['CandidateID'] = df['Name link'].str.extract('(\d+)', expand=False)
    df['PartyID'] = df['Party link'].str.extract('(\d+)', expand=False)
    votes_share_df = df['Votes'].str.extract('(?P<votes>\d+,?\d*) \((?P<share>\d+\.?\d*)%\)', expand=False)
    df['Votes'] = votes_share_df['votes'].str.replace(',', '').astype(int)
    df['Share'] = votes_share_df['share'].astype(float)
    to_drop = ["Votes link", "Photo", "Entry Date", "Margin", "Predict Avg.", "Predict Avg. link", "Margin link",
               "Entry Date link", "Cash On Hand", "Cash On Hand link", "Name", "Website link", 'Name link',
               'Party link']
    df.drop(to_drop, axis=1, inplace=True)
    return df


def check_share_sum(df):
    df_sum = df.groupby(['RaceID'])['Share'].sum().reset_index()
    bad_rows = df_sum[abs(df_sum['Share'] - 100) >= .1]
    if len(bad_rows) == 0:
        print 'no bad rows'
    else:
        print 'bad rows:'
        print bad_rows


def fix_bad_share(df):
    epsilon = 1e-10
    df['Share'] += epsilon
    df_sum = df.groupby(['RaceID'])['Share'].sum().reset_index()
    df_sum.rename(columns={'Share': 'Total'}, inplace=True)
    df_m = df.merge(df_sum, left_on='RaceID', right_on='RaceID')
    df_m['Share'] = 100 * df_m['Share'] / df_m['Total']
    df_m.drop(['Total'], axis=1, inplace=True)
    return df_m


def merge_city(df):
    names = ['web', 'city', 'state', 'partisan', 'note']
    df_city = pd.read_csv('../../data/input/recent_elections_city.csv', header=None, names=names)
    df_city['CityID'] = range(len(df_city))
    df_city.drop('note', axis=1, inplace=True)
    df_city['RaceID'] = df_city['web'].str.extract('(\d+)', expand=False)
    return df.merge(df_city, left_on='RaceID', right_on='RaceID')


def merge_state(df):
    names = ['ContainerID', 'State', 'year', 'note']
    df_state = pd.read_csv('../../data/input/recent_elections_state.csv', header=None, names=names)
    df_state['StateID'] = range(len(df_state))
    df_state.drop('note', axis=1, inplace=True)
    return df.merge(df_state, left_on='State', right_on='State')
