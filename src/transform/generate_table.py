import re
import json
from datetime import datetime
import pandas as pd
from glob import glob
from utils import keep_ascii

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import JSON_DIR, ENTRY_DIR


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


def generate_race_details_table(position):
    df = select_tables(JSON_DIR, position, 'RaceDetails')
    date_cols = ['Polls Close', 'Term Start', 'Term End']
    for column_title in date_cols:
        df[column_title] = pd.to_datetime(df[column_title].str.split('-').str[0], errors='coerce')
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
               "Entry Date link", "Cash On Hand", "Cash On Hand link", "Website","Website link", 'Name link',
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
    for row in city_name_correspondence[['City', 'city_name']]:
        city_name_lookup[row[0]] = row[1]
    adjust_name(df, 'City', city_name_lookup)
    return df.merge(df_city[['city_name', 'CityID']], left_on='City', right_on='city_name')


def merge_state(df):
    names = ['ContainerID', 'State', 'year', 'note']
    df_state = pd.read_csv(os.path.join(ENTRY_DIR, 'recent_elections_state.csv'), header=None, names=names)
    df_state['StateID'] = range(len(df_state))
    df_state.drop('note', axis=1, inplace=True)
    df_state.loc[:,'State'] = df_state['State'].str.replace('_',' ').str.title()
    return df.merge(df_state, left_on='State', right_on='State')


if __name__ == '__main__':
    from StopWatch import StopWatch
    from utils import check_share_sum, fix_bad_share
    from clean_data import terminal_election, early_dist,key_election,win_follow_ever,\
                           incumbent_election_v1, incumbent_election_v2, career_span, first_win,\
                           winner_follower
    from stat_data import select_dist, statistics_dist, statistics_election, statistics_candidates
    from df2tex import df2tex

    lookup_office = {'CityID':'Mayor','StateID':'Governor'}
    distID = 'StateID'

    sw = StopWatch()
    df1 = generate_race_details_table(position=lookup_office[distID])
    print df1.shape, df1.head()
    sw.tic('generate race details table')
    df2 = generate_candidate_table(position=lookup_office[distID])
    print df2.shape, df2.head()
    sw.tic('generate candidate table')
    print 'before'
    check_share_sum(df2)
    df_m = fix_bad_share(df2)
    print 'after'
    check_share_sum(df_m)

    if distID == 'CityID':
        df_race_all = merge_city(df1)
        df12 = df1.merge(df2, left_on="RaceID",right_on="RaceID",how="outer")
        df_race2_all = merge_city(df12)
    else:
        df_race_all = merge_state(df1)
        df12 = df1.merge(df2, left_on="RaceID", right_on="RaceID", how="outer")
        df_race2_all = merge_state(df12)

    todrop = ['22593', '191', '19359', '30530', '4666','4667']
    for x in todrop:
        df_race2_all = df_race2_all.drop(df_race2_all[df_race2_all['CandID'] == x].index)

    df_race2_all = df_race2_all[~df_race2_all['Term End'].isnull()]

    df_race2_all = terminal_election(df_race2_all, distID)  # Terminal race per election period
    df_race2_all = early_dist(df_race2_all, distID) # First documented race per city

    df_win = df_race2_all[df_race2_all['Terminal'] == 1.0]
    df_race2_all = winner_follower(df_race2_all, df_win,distID, 'winner') # Indicator for winner per terminal race

    df_follow = df_race2_all[(df_race2_all['Terminal'] == 1.0) & (df_race2_all['winner'] == 0.0)]
    df_race2_all = winner_follower(df_race2_all, df_win, distID, 'follower') # Indicator for follower per terminal race

    df_race2_all = key_election(df_race2_all, distID) # Key race per election period by turnout

    df_win_key = df_race2_all[df_race2_all['KeyRace'] == 1.0]
    df_race2_all = winner_follower(df_race2_all, df_win_key, distID,
                                   'winner_key')  # Indicator for winner per key race
    df_follow_key = df_race2_all[(df_race2_all['KeyRace'] == 1.0) & (df_race2_all['winner_key'] == 0.0)]
    df_race2_all = winner_follower(df_race2_all, df_follow_key, distID,
                                   'follower_key')  # Indicator for follower per key race

    df_race2_all = win_follow_ever(df_race2_all) # Indicator for if ever win/follow a race
    df_race2_all = incumbent_election_v1(df_race2_all,distID) # If an incumbent exists
    df_race2_all = incumbent_election_v2(df_race2_all, distID) # If an incumbent exists
    df_race2_all = career_span(df_race2_all) # Date for first try and last try
    df_race2_all = first_win(df_race2_all) # Date for first winning

    df_race2_all.to_csv("../../data/df_race2_all.csv")

    df_race2_all = select_dist(df_race2_all, [[distID,1000]], [['Term Start', datetime(1990, 1, 1)]] )
    stat_dist = statistics_dist(df_race_all,distID)
    stat_election = statistics_election(df_race2_all,distID)
    stat_cand = statistics_candidates(df_race2_all)

    row_name = [
        ['stat_dist','N', 'N with Data', 'Avg Ranks (Unweighted)', 'Avg Ranks (Weighted by Periods)',
                     'Avg Election Periods', 'Avg Elections', 'Avg Term Lengths'],
        ['stat_election','Elections Covered', 'Election Periods Covered', 'Incumbent Election Periods',
                         'Incumbent Election Candidates', 'Open Election Periods', 'Open Election Candidates',
                         'Unclear Election Periods', 'Unclear Election Candidates'],
        ['stat_cand', 'Number of Unique Candidates', 'Number of Election Periods Per Candidate',
                      'Number of Candidates at least winning once', 'Number of Candidates never win',
                      'Winners: Number of Election Periods','Winners: Number of Winning Election Periods',
                      'Winners: Number of Failed Tries before First Win','Winners: Number of Tries After First Win',
                      'Winners: Number of Wins After First Win','Winners: Number of Fails After First Win',
                      'Losers: Number of Election Periods', ]
        ]
    for index, stats in enumerate([stat_dist, stat_election, stat_cand]):
        row_head = row_name[index][0]
        row_tail = row_name[index][1:]
        df = pd.DataFrame({'Variable': stats.keys(), 'Value': stats.values()}).set_index('Variable', drop=False)
        df = df.reindex(row_tail)
        df2tex(df,'/Users/yuwang/Dropbox/local politicians/model/analysis_{}/'.format(lookup_office[distID]), '{}.tex'.format(row_head), "%8.2f", 0, ['Value'], ['Variable'], ['Variable', 'Value'])

