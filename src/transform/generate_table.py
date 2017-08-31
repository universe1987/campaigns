import re
import json
import pandas as pd
from glob import glob
from utils import keep_ascii

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import JSON_DIR, ENTRY_DIR

from StopWatch import StopWatch
from utils import check_share_sum, fix_bad_share
from clean_data import *
from stat_data import *



# Merge Race Details
def select_tables(folder, office, table_title):
    content = {'RaceID': []}
    input_file_pattern = '{}/race*{}*{}.json'.format(folder, office, table_title)
    if office == 'All':
        input_file_pattern = '{}/race*{}.json'.format(folder,table_title)
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


def generate_race_details_table(position):
    df = select_tables(JSON_DIR, position, 'RaceDetails')
    date_cols = ['Polls Close', 'Term Start', 'Term End']
    for column_title in date_cols:
        df[column_title] = pd.to_datetime(df[column_title].str.split('-').str[0].str.replace('00,', '01,'),
                                          errors='coerce')
    df['State'] = df['Parents'].str.split('>').str[2].str.strip()
    adjust_name(df, 'State', {"Hawai'i": "Hawaii", "Territory of Alaska": "Alaska", "Territory of Hawai'i": "Hawaii"})
    if position == 'Mayor':
        df['City'] = df['Parents'].str.split('>').str[-2].str.strip()
    df['Position'] = df['Parents'].str.split('>').str[-1].str.strip()
    df['Term Length'] = df['Term End'] - df['Term Start']
    df['ContributorID'] = df['Contributor link'].str.extract('(\d+)', expand=False)
    df['Data Sources'] = df['Data Sources'].str.replace('\[Link\]', "")
    df['Turnout'] = df['Turnout'].str.extract('(\d+\.?\d*)% Total Population', expand=False).astype(float)
    to_drop = ['Contributor link', 'Filing Deadline', 'Filing Deadline link', 'Last Modified', 'Last Modified link',
               'Office link', 'Parents link', 'Polls Close link', 'Polls Open', 'Polls Open link', 'Term End link',
               'Term Start link', 'Turnout link', 'Type link', 'Data Sources link']
    df.drop(to_drop, axis=1, inplace=True)
    return df


def generate_candidate_table(position):
    df = select_tables(JSON_DIR, position, 'Candidates')
    df = df[df['Name'] != '']
    df['CandID'] = df['Name link'].str.extract('(\d+)', expand=False)
    df['PartyID'] = df['Party link'].str.extract('(\d+)', expand=False)
    votes_share_df = df['Votes'].str.extract('(?P<votes>\d+,?\d*) \((?P<share>\d+\.?\d*)%\)', expand=False).fillna('-1')
    df['Votes'] = votes_share_df['votes'].str.replace(',', '').astype(int)
    df['Share'] = votes_share_df['share'].astype(float)
    df = df[df['Share'] >= 0]
    to_drop = ["Votes link", "Photo", "Entry Date", "Margin", "Predict Avg.", "Predict Avg. link", "Margin link",
               "Entry Date link", "Cash On Hand", "Cash On Hand link", "Website", "Website link", 'Name link',
               'Party link']
    df.drop(to_drop, axis=1, inplace=True)
    return df


def adjust_name(df, column_name, lookup):
    for i in xrange(len(df)):
        k = df[column_name][i]
        if k in lookup:
            df.loc[i, column_name] = lookup[k]


def merge_city(df):
    names = ['url', 'city_name', 'state', 'partisan', 'note']
    df_city = pd.read_csv(os.path.join(ENTRY_DIR, 'recent_elections_city.csv'), header=None, names=names)
    df_city['CityID'] = range(len(df_city))
    df_city.drop('note', axis=1, inplace=True)
    df_city['RaceID'] = df_city['url'].str.extract('(\d+)', expand=False)

    city_name_lookup = {}
    city_name_correspondence = pd.merge(df[['City', 'RaceID']], df_city[['city_name', 'RaceID']])
    for row in city_name_correspondence[['City', 'city_name']].values:
        city_name_lookup[row[0]] = row[1]
    adjust_name(df, 'City', city_name_lookup)
    return df.merge(df_city[['city_name', 'CityID', 'state']], left_on=['City', 'State'],
                    right_on=['city_name', 'state'])


def merge_state(df):
    names = ['ContainerID', 'State', 'year', 'note']
    df_state = pd.read_csv(os.path.join(ENTRY_DIR, 'recent_elections_state.csv'), header=None, names=names)
    df_state['StateID'] = range(len(df_state))
    df_state.drop('note', axis=1, inplace=True)
    df_state.loc[:, 'State'] = df_state['State'].str.replace('_', ' ').str.title()
    return df.merge(df_state, left_on='State', right_on='State')


if __name__ == '__main__':
    lookup_office = {'CityID': 'Mayor', 'StateID': 'Governor','AllID':'All'}
    for distID in ['AllID','CityID','StateID']:
        sw = StopWatch()
        df_race_details = generate_race_details_table(position=lookup_office[distID])
        sw.tic('generate race details table')
        df_candidate = generate_candidate_table(position=lookup_office[distID])
        sw.tic('generate candidate table')
        check_share_sum(df_candidate)
        df_m = fix_bad_share(df_candidate)
        check_share_sum(df_m)

        if distID == 'CityID':
            df_tmp = merge_city(df_race_details)
            for s in ['Vice Mayor', 'Mayor Pro Tem']:
                df_tmp = df_tmp[df_tmp['Position'] != s]
        elif distID == 'StateID':
            df_tmp = merge_state(df_race_details)
            df_tmp = df_tmp[df_tmp['Position'] == 'Governor']
        elif distID == 'AllID':
            df_tmp = df_race_details

        print df_tmp.shape
        df_tmp = df_tmp[df_tmp['Type'].str.contains('Election')]
        print df_tmp.shape
        df_tmp = df_tmp[df_tmp['Term End'] - df_tmp['Term Start'] > timedelta(days=360)]
        print df_tmp.shape
        df_all = df_tmp.merge(df_m, left_on="RaceID", right_on="RaceID")

        todrop = ['3567','22593', '191', '19359', '30530', '4666', '4667']
        for x in todrop:
            df_all = df_all.drop(df_all[df_all['CandID'] == x].index)

        df_all = df_all[~df_all['Term End'].isnull()]

        if distID in ['CityID','StateID']:
            df_all = term_start_merge(df_all, distID, timedelta(days=120))
            print 'Total =', df_all.groupby([distID])['Term Start'].nunique().reset_index().sum()
            # Package close-date elections in one election period
            # Frequent data-input errors: 'Term Start' differs in primary and general elections

            df_all = terminal_election(df_all, distID)  # Terminal race per election period
            df_all = early_dist(df_all, distID)  # First documented race per city

            df_win = df_all[df_all['Terminal']]
            df_all = winner_follower(df_all, df_win, distID, 'winner')  # Indicator for winner per terminal race

            df_follow = df_all[df_all['Terminal'] & ~df_all['winner']]
            df_all = winner_follower(df_all, df_follow, distID, 'follower')  # Indicator for follower per terminal race

            df_all = key_election(df_all, distID)  # Key race per election period by turnout

            df_win_key = df_all[df_all['KeyRace']]
            df_all = winner_follower(df_all, df_win_key, distID, 'winner_key')  # Indicator for winner per key race
            df_follow_key = df_all[df_all['KeyRace'] & ~df_all['winner_key']]
            df_all = winner_follower(df_all, df_follow_key, distID, 'follower_key')  # Indicator for follower per key race

            df_all = win_follow_ever(df_all)  # Indicator for if ever win/follow a race
            df_all = incumbent_election_v1(df_all, distID)  # If an incumbent exists
            df_all = incumbent_election_v2(df_all, distID)  # If an incumbent exists
            df_all = career_span(df_all)  # Date for first try and last try
            df_all = first_win(df_all)  # Date for first winning

        df_all.to_csv("../../data/pdata/df_all_{}.csv".format(distID))
        df_all.to_pickle("../../data/pdata/df_all_{}.pkl".format(distID))

