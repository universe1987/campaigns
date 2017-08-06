import os
import re
import csv
import json
import time
import collections
import numpy as np
import pandas as pd
from glob import glob


# Merge Race Details
def key_race_details(folder, office, table_title, output_file):
    content = {}
    input_file_pattern = '{}/race*{}*{}.json'.format(folder, office, table_title)
    for filename in glob(input_file_pattern):
        race_id = re.findall('\d+', filename)[1]
        with open(filename, 'rb') as fp:
            son = json.load(fp)
            n_new_row = None
            for key, values in son.iteritems():
                if "ertified Votes" in key:
                    key = "Votes"
                key_text = key + '_text'
                if key_text not in content:
                    content[key_text] = []
                content[key_text].extend([v['text'] for v in values])
                key_link = key + '_link'
                if key_link not in content:
                    content[key_link] = []
                content[key_link].extend([v['link'] for v in values])
                if n_new_row:
                    assert n_new_row == len(values)
                n_new_row = len(values)
            content['RaceID'] += [race_id] * n_new_row

    header = content.keys()
    n_row = [len(content[h]) for h in header]
    assert min(n_row) == max(n_row)

    with open(output_file, 'wb') as fp:
        csvwriter = csv.writer(fp)
        csvwriter.writerow(header)
        for i in xrange(len(content['RaceID'])):
            csvwriter.writerow([content[h][i] for h in header])


def clean_null(df, col, null_words):
    # To remove rows if a certain column contains elements in the list of null words
    # df is a loaded csv file, i.e. df = pd.read_csv(file.csv)
    print 'Before clean_null:', len(df['Name'])
    for x in null_words:
        df = df[df[col] != x]
    print 'After clean_null:', len(df['Name'])
    return df


def split_two(df, dic):
    for key, value in dic.iteritems():
        df['temp1'], df[value[0]] = df[key].str.split("u'text':").str
        df[value[0]], df['temp1'] = df[value[0]].str.split("', u'link':").str
        df[value[0]] = df[value[0]].str.replace("u'", '')
        df['temp1'], df[value[1]] = df[key].str.split("u'link':").str
        df[value[1]] = df[value[1]].str.replace("u'", '')
        df[value[1]], df['temp1'] = df[value[1]].str.split("'\}").str
        df = df.drop('temp1', 1)
    return df


def keep_ascii(s):
    ascii_part = [c for c in s if ord(c) < 128]
    x = ''.join(ascii_part).strip()
    return ' '.join(x.split())


def date_yr_mon(df, dic):
    for key, value in dic.iteritems():
        df[key] = df[key].str.replace('(?:-).*', '').str.replace("00,", "01,")
        df.ix[df[key].str.startswith('(\d+)'), key] = "January" + df[key].astype(str)
        df[key + '_date'] = pd.to_datetime(df[key], errors='coerce')
        df[value[0]] = df[key + '_date'].apply(lambda x: x.year)
        df[value[1]] = df[key + '_date'].apply(lambda x: x.month)
        df = df.drop([key], 1)
    return df


def state_county_city(df):
    df['count'] = df['Parent'].str.count('>')
    df['c1'], df['c2'], df['c3'], df['c4'], df['c5'], df['c6'], df['c7'] = df['Parent'].str.split('>').str
    dic = {6: ['c3', 'c5', 'c6'], 5: ['c3', 'c5', 'c5'], 4: ['c3', 'c4', 'c4']}
    df['State'] = df['County'] = df['City'] = ""
    for key, value in dic.iteritems():
        df.ix[df['count'] == key, 'State'] = df[value[0]]
        df.ix[df['count'] == key, 'County'] = df[value[1]]
        df.ix[df['count'] == key, 'City'] = df[value[2]]
    list = ['c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'Parent', 'count']
    df = df.drop(list, 1)
    return df


def split_votes_share(df, dic):
    for key, value in dic.iteritems():
        df[value[0]], df[value[1]] = df[key].str.split("(").str
        df[value[0]] = df[value[0]].apply(keep_ascii)
        df[value[1]], df['temp'] = df[value[1]].str.split("%").str
        df = df.drop('temp', 1)
    return df


def setup_race_details():
    start = time.time()
    df = pd.read_csv('key_race_details.csv')

    dics = {'Contributor': ['Contributor Name', 'ContributorID'],
            'Data Sources': ['Source', 'Source Link'],
            'Office': ['Offices', 'v1'],
            'Parents': ['Parent', 'v2'],
            'Polls Close': ['Polls Closes', 'v3'],
            'Term Start': ['Term Starts', 'v5'],
            'Term End': ['Term Ends', 'v4'],
            'Type': ['Turnout', 'v6'],
            'Append0': ['Types', 'v7']}
    df = split_two(df, dics)

    list = dics.keys() + ['Filing Deadline', 'Last Modified', 'Polls Open',
                          'v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7']
    df = df.drop(list, 1)

    dic = {'Offices': 'Office', 'Polls Closes': 'Polls Close', "Term Starts": "Term Start",
           'Term Ends': 'Term End', 'Types': 'Type'}
    for key, value in dic.iteritems():
        df = df.rename(columns={key: value})

    list = ['ContributorID']
    for x in list:
        df[x] = df[x].str.extract('(\d+)', expand=False)

    df['Source'] = df['Source'].str.replace('\[Link\]', "")

    df.loc[df['Type'] == "", 'Type'] = df['Turnout']
    df['Turnout'] = df['Turnout'].str.extract('(\d+.\d+)', expand=False).astype(float)

    dic = {"Term Start": ["Term Start Year", "Term Start Month"],
           "Term End": ["Term End Year", "Term End Month"],
           "Polls Close": ["Poll Year", "Poll Month"]}
    df = date_yr_mon(df, dic)

    df = state_county_city(df)

    df['Term Length'] = df['Term End Year'] - df['Term Start Year']

    df.to_csv("test3.csv")

    print df.head(10)
    end = time.time()
    print("Race Details 1 is finished", end - start, 'elapsed')
    return df


def setup_race_details2():
    start = time.time()
    df = pd.read_csv('key_race_details2.csv')
    df = clean_null(df, 'Name', ["{u'text': u'', u'link': u''}"])

    dics = {'Name': ['Names', 'CandID'],
            'Final Votes': ['Votes_Share', 'v1'],
            'Party': ['Partys', 'PartyID'],
            'Website': ['v2', 'Web']}
    df = split_two(df, dics)

    list = ['CandID', 'PartyID']
    for x in list:
        df[x] = df[x].str.extract('(\d+)', expand=False)

    dics = {'Votes_Share': ['Votes', 'Share']}
    df = split_votes_share(df, dics)

    list = ["Votes_Share", "Photo", "Entry Date", "Margin", "Predict Avg.",
            "Cash On Hand", "Name", "Final Votes", "Party", "Website", "v1", "v2"]
    df = df.drop(list, 1)

    dic = {'Names': 'Name', 'Partys': 'Party'}
    for key, value in dic.iteritems():
        df = df.rename(columns={key: value})

    df.to_csv("test4.csv")

    print df.head(13)
    end = time.time()
    print ("Race Details 2 is finished", end - start, 'elapsed')
    return df


def check_shares_sum():
    # add up the shares in a file which contains votes, shares, per election
    def add_shares(df_race2):
        df_race2['Share'] = df_race2['Share'].astype(float)
        df = df_race2.groupby(['RaceID'])['Share'].sum().reset_index()
        df['Index'] = range(df.shape[0])
        # df.to_csv("test5.csv")
        for x in [10, 50, 90, 98, 101, 1000]:
            print "<", x, len(df[(df['Share'] < x)])
        df_race2_wrong_shares = df[df['Share'] < 50]
        return df_race2_wrong_shares

    # return a full list of incorrect races
    def shares_wrong_big(df_race2_wrong_shares, df_race, df_race2):
        # df is part of df_race2 with incorrect shares
        df_race2_wrong_shares['RaceID'] = df_race2_wrong_shares['RaceID'].astype(int)
        df_race['RaceID'] = df_race['RaceID'].astype(int)
        df = df_race.merge(df_race2_wrong_shares, left_on='RaceID', right_on='RaceID', how='outer')
        df_wrong_shares_full = df_race2.merge(df, left_on='RaceID', right_on='RaceID', how='outer')
        df_wrong_shares_full['Share_y'] = df_wrong_shares_full['Share_y'].astype(str)
        df_wrong_shares_full = df_wrong_shares_full[df_wrong_shares_full['Share_y'].str.contains(r'\d+')]
        df_wrong_shares_full.to_csv("wrong_shares_full.csv")
        return df_wrong_shares_full

    # return a list of RaceIDs for incorrect races
    def shares_wrong_small(df_wrong_shares_full):
        g = df_wrong_shares_full['RaceID'].unique()
        s = pd.Series(g)
        s.to_csv('wrong_shares_raceid.csv')

    df_race2_wrong_shares = add_shares(df_race2)
    df_wrong_shares_full = shares_wrong_big(df_race2_wrong_shares, df_race, df_race2)
    shares_wrong_small(df_wrong_shares_full)
    return df_wrong_shares_full


def unique_candidates(df_race2):
    g = df_race2['CandID'].unique()
    df_unique_CandID = pd.DataFrame(pd.Series(g)).rename(columns={0: 'CandID'})
    print 'number of unique candidates=', len(df_unique_CandID)
    df_unique_CandID.to_csv('unique_CandID.csv')
    return df_unique_CandID


def cand_remove(df, list):
    for x in list:
        df = df[df['CandID'] != x]
    return df


def recent_elections_city():
    path = r'{}'.format(dir6)
    df_m = pd.DataFrame()
    for id in [1, 2, 3]:
        df = pd.read_csv(path + "/recent_elections_part{}.txt".format(id), delimiter=';', header=None)
        df_m = df_m.append(df)
    h = df_m.shape[0]
    df_m['CityID'] = range(h)
    dic = {0: 'web', 1: 'city', 2: "state", 3: 'partisan', 4: 'note'}
    for key, value in dic.iteritems():
        df_m = df_m.rename(columns={key: value})
    df_m.to_csv('recent_elections.csv')
    df_m = df_m.drop('note', 1)
    return df_m


def city_name_merge(df_recent, df_race):
    df_recent['RaceID'] = df_recent['web'].str.extract('(\d+)', expand=False).astype(str)
    df_race['RaceID'] = df_race['RaceID'].astype(str)
    df = df_recent.merge(df_race, left_on='RaceID', right_on='RaceID', how='outer')
    df_city = df[['State', 'County', 'City', 'web', 'state', 'city', 'CityID']]
    df_city.loc[:, 'CityID'] = df_city['CityID'].astype(str)
    df_city = df_city[df_city['CityID'].str.contains('(\d+)')]
    df_city = df_city[df_city['web'].str.contains('http')]
    df_city.to_csv('city_name_merge.csv')
    return df_city


def race_details_recent(df_race, df_dist, distID, label):
    df_race_all = df_race.merge(df_dist, left_on=label, right_on=label, how='outer')
    df_race_distID = df_race_all.groupby([distID])['RaceID'].count().reset_index()
    print df_race_distID['RaceID'].describe()
    df_race_all.to_csv('race_details_all_v0.csv')
    return df_race_all


def race_details2_recent(df_non_writein, df_race_all, distID):
    df_non_writein.loc[:, 'RaceID'] = df_non_writein['RaceID'].astype(str)
    df_race_all.loc[:, 'RaceID'] = df_race_all['RaceID'].astype(str)
    df_race2_all = df_non_writein.merge(df_race_all, left_on=['RaceID'], right_on=['RaceID'], how='outer')
    df_race2_distID = df_race2_all.groupby([distID])['CandID'].count().reset_index()
    print df_race2_distID['CandID'].describe()
    df_race2_all.to_csv('race_details2_all_v0.csv')
    return df_race2_all


def terminal_election(df_race2_all, distID):
    df_terminal = df_race2_all.groupby([distID, 'Term Start Year'])['Polls Close_date'].max().reset_index() \
        .rename(columns={'Polls Close_date': 'Terminal Date'})
    df_race2_all = df_race2_all.merge(df_terminal, left_on=[distID, 'Term Start Year'],
                                      right_on=[distID, 'Term Start Year'], how='outer')
    df_race2_all['Terminal'] = (df_race2_all['Polls Close_date'] == df_race2_all['Terminal Date'])
    return df_race2_all


def early_city(df_race2_all, distID):
    df_early = df_race2_all.groupby([distID])['Term Start Year'].min().reset_index() \
        .rename(columns={'Term Start Year': 'Earlist Date'})
    df_race2_all = df_race2_all.merge(df_early, left_on=[distID], right_on=[distID], how='outer')
    df_race2_all['Earlist'] = (df_race2_all['Term Start Year'] == df_race2_all['Earlist Date'])
    return df_race2_all


def winner_election_period(df_race2_all, distID):
    df_winner = df_race2_all[df_race2_all['Terminal'] == True]
    df_winner = df_winner.groupby([distID, 'Term Start Year'])['Votes'].max().reset_index() \
        .rename(columns={'Votes': 'Votes Max'})
    df_race2_all = df_race2_all.merge(df_winner, left_on=[distID, 'Term Start Year'],
                                      right_on=[distID, 'Term Start Year'], how='outer')
    df_race2_all['winner'] = (df_race2_all['Votes'] == df_race2_all['Votes Max'])
    df_race2_all['winnerID'] = (df_race2_all['Votes'] == df_race2_all['Votes Max']) * 1.0 * df_race2_all[
        'CandID'].astype(float)
    return df_race2_all


def incumbent_election_v1(df_race2_all, distID):
    df_race2_all['Name Flag'] = (df_race2_all['Name'].str.contains('I')) * 1.0
    df = df_race2_all.groupby([distID, 'Term Start Year'])['Name Flag'].sum().reset_index()
    df['Incumbent1'] = (df['Name Flag'] > 0.0) * 1.0
    df_race2_all = df_race2_all.merge(df, left_on=[distID, 'Term Start Year'], right_on=[distID, 'Term Start Year'],
                                      how='outer')
    return df_race2_all


def incumbent_election_v2(df_race2_all, distID):
    df_winner_id = df_race2_all[['winnerID', distID, 'Term Start Year']] \
        .groupby([distID, 'Term Start Year'])['winnerID'].max().reset_index() \
        .sort_values([distID, 'Term Start Year'], ascending=True)
    df_winner_id['winnerID previous'] = df_winner_id.groupby([distID])['winnerID'].shift(1)
    df_winner_id = df_winner_id.drop('winnerID', 1)

    df_race2_all = df_race2_all.merge(df_winner_id, left_on=[distID, 'Term Start Year'],
                                      right_on=[distID, 'Term Start Year'], how='outer')
    df_race2_all['CandID'] = df_race2_all['CandID'].astype(float)
    df_race2_all['winnerID previous'] = df_race2_all['winnerID previous'].astype(float)
    df_race2_all['Matched'] = (df_race2_all['CandID'] == df_race2_all['winnerID previous']) * 1.0

    df = df_race2_all.groupby([distID, 'Term Start Year'])['Matched'].max().reset_index()
    df = df[df['Matched'] > 0]
    df = df.rename(columns={'Matched': 'Incumbent2'})
    df_race2_all = df_race2_all.merge(df, left_on=[distID, 'Term Start Year'], right_on=[distID, 'Term Start Year'],
                                      how='outer')
    df_race2_all.loc[df_race2_all['Incumbent2'] != 1, 'Incumbent2'] = (df_race2_all['Earlist']) * 2.0
    # Incumbent2 = {1:incumbent, 0:open, 2:unclear}

    return df_race2_all


def statistics_dist(df_recent, df_dist, df_periods, df_race_all, dist, dists, distID):
    stat_dist = dict()

    s = len(df_recent[dist])
    print 'Total {}'.format(dists), s
    stat_dist['Total {}'.format(dists)] = s

    s = len(df_recent[df_recent['web'].str.contains('http')])
    print 'Total {} with Data'.format(dists), s
    stat_dist['Total {} with Data'.format(dists)] = s

    df_dist[distID] = df_dist[distID].astype(float)
    s = df_dist[distID].mean()
    print 'Avg Ranks', s
    stat_dist['avg Ranks'] = s

    s = df_dist[distID].median()
    print 'Median Ranks', s
    stat_dist['Median Ranks'] = s

    df_periods_dist = df_periods.groupby([distID])['Term Start Year'].count().reset_index()
    s = df_periods_dist['Term Start Year'].mean()
    print 'Avg Election Periods by {}:'.format(dist), s
    stat_dist['Avg Election Periods'] = s

    df_elections_dist = df_race_all.groupby([distID])['RaceID'].count().reset_index()
    s = df_elections_dist['RaceID'].mean()
    print 'Avg Elections by {}:'.format(dist), s
    stat_dist['Avg Elections'] = s

    df_term_dist = df_race_all.groupby([distID])['Term Length'].mean().reset_index()
    s = df_term_dist['Term Length'].mean()
    print 'Avg Term Length:', s
    stat_dist['Avg Term Lengths'] = s

    return stat_dist


def statistics_election(df_periods, df_race2_all, distID):
    stat_election = dict()

    s = df_periods['RaceID'].sum()
    print 'Elections Covered', s
    stat_election['Election Covered'] = s

    s = df_periods['RaceID'].count()
    print 'Number of Election Periods', s
    stat_election['Election Periods Covered'] = s

    df = df_race2_all[df_race2_all['Incumbent2'] == 1]
    df = df.groupby([distID, 'Term Start Year'])['RaceID'].max().reset_index()
    s = df['Term Start Year'].count()
    print 'Number of Incumbent Election Periods', s
    stat_election['Incumbent Election Periods'] = s

    df2 = df_race2_all[df_race2_all['Incumbent2'] == 1]
    g = df2['RaceID'].unique()
    s = pd.Series(g).count()
    print 'Number of Unique Incumbent Candidates', s
    stat_election['Incumbent Election Candidates'] = s

    df = df_race2_all[df_race2_all['Incumbent2'] == 0]
    df = df.groupby([distID, 'Term Start Year'])['RaceID'].max().reset_index()
    s = df['Term Start Year'].count()
    print 'Number of Open Election Periods', s
    stat_election['Open Election Periods'] = s

    df2 = df_race2_all[df_race2_all['Incumbent2'] == 0]
    g = df2['RaceID'].unique()
    s = pd.Series(g).count()
    print 'Number of Unique Open Candidates', s
    stat_election['Open Election Candidates'] = s

    df = df_race2_all[df_race2_all['Incumbent2'] == 2]
    df = df.groupby([distID, 'Term Start Year'])['RaceID'].max().reset_index()
    s = df['Term Start Year'].count()
    print 'Number of Unclear Election Periods', s
    stat_election['Unclear Election Periods'] = s

    df2 = df_race2_all[df_race2_all['Incumbent2'] == 2]
    g = df2['RaceID'].unique()
    s = pd.Series(g).count()
    print 'Number of Unique Unclear Candidates', s
    stat_election['Unclear Election Candidates'] = s

    return stat_election


def select_districts(df_race2_all, key1, cutoff1, key2, cutoff2):
    df_race2_all.loc[:, key1] = df_race2_all[key1].astype(float)
    print len(df_race2_all)
    df_race2_all = df_race2_all[df_race2_all[key1] < cutoff1]
    df_race2_all = df_race2_all[df_race2_all[key2] > cutoff2]
    print len(df_race2_all)
    df_non_writein_id = df_race2_all.groupby(['CandID'])['RaceID'].count().reset_index().rename(
        columns={'RaceID': 'RaceIDs'})
    return df_race2_all, df_non_writein_id


def statistics_candidates():
    def cand_inc_cha_open(df_race2_all, df_non_writein_id):
        df_race_ct = df_non_writein_id
        dic0 = {'elections': 'RaceID', 'election periods': 'Term Start Year'}
        dic1 = {'Open': df_race2_all['Incumbent2'] == 0,
                'Incumbent': (df_race2_all['Incumbent2'] == 1) & (
                    df_race2_all['CandID'] == df_race2_all['winnerID previous']),
                'Challenger': (df_race2_all['Incumbent2'] == 1) & (
                    df_race2_all['CandID'] != df_race2_all['winnerID previous']),
                'Unclear': df_race2_all['Incumbent2'] == 2}
        for label0, value0 in dic0.iteritems():
            for label1, value1 in dic1.iteritems():
                df = df_race2_all[value1].groupby(['CandID'])[value0].nunique().reset_index().rename(
                    columns={value0: '{} {}'.format(label1, label0)})
                df_non_writein_id.loc[:, 'CandID'] = df_non_writein_id['CandID'].astype(float).astype(str)
                df.loc[:, 'CandID'] = df['CandID'].astype(float).astype(str)
                df2 = df_non_writein_id.merge(df, left_on='CandID', right_on='CandID', how='outer')
                df2.loc[df2['{} {}'.format(label1, label0)].isnull(), '{} {}'.format(label1, label0)] = 0
                s = df2['{} {}'.format(label1, label0)].mean()
                stat_cand['Avg number of {} {}'.format(label1, label0)] = s
                df_race_ct = df_race_ct.merge(df2, left_on='CandID', right_on='CandID', how='outer')
        return ()

    def win_lose(df_race2_all):
        df_winner_list = df_race2_all.groupby(['winnerID'])['Term Start Year'].nunique().reset_index().rename(
            columns={'winnerID': 'CandID', 'Term Start Year': 'Win at once (Wins)'})
        df_winner_list = df_winner_list[df_winner_list['CandID'] > 0.0]
        s1 = len(df_winner_list)
        s2 = df_winner_list['Win at once (Wins)'].mean()

        df_race2_all_winner = df_race2_all.merge(df_winner_list, left_on='CandID', right_on='CandID', how='right')
        df = df_race2_all_winner.groupby(['CandID'])['Term Start Year'].nunique().reset_index()
        s3 = df['Term Start Year'].mean()

        df = df_race2_all.merge(df_winner_list, left_on='CandID', right_on='CandID', how='outer')
        df_race2_all_loser = df[df['Win at once (Wins)'].isnull()]
        df_loser_list = df_race2_all_loser.groupby(['CandID'])['Term Start Year'].nunique().reset_index()
        s4 = len(df_loser_list)
        s5 = df_loser_list['Term Start Year'].mean()

        df = df_race2_all_winner[df_race2_all_winner['CandID'] == df_race2_all_winner['winnerID']].groupby(['CandID'])[
            'Term Start Year'].min().reset_index() \
            .rename(columns={'Term Start Year': 'First Win Year'})
        df_race2_all_winner_1st = df_race2_all_winner.merge(df, left_on='CandID', right_on='CandID', how='outer')

        return s1, s2, s3, s4, s5, df_winner_list, df_race2_all_winner, df_race2_all_loser, df_race2_all_winner_1st

    def win_once_early_fails(df_race2_all_winner_1st, df_winner_list):
        df = df_race2_all_winner_1st[
            df_race2_all_winner_1st['Term Start Year'] < df_race2_all_winner_1st['First Win Year']]
        df = df.groupby(['CandID'])['Term Start Year'].nunique().reset_index().rename(
            columns={'Term Start Year': 'Win at once (Early Fails)'})
        df = df_winner_list.merge(df, left_on='CandID', right_on='CandID', how='left')
        df.loc[df['Win at once (Early Fails)'].isnull(), 'Win at once (Early Fails)'] = 0.0
        s = df['Win at once (Early Fails)'].mean()
        return s

    def win_once_late_tries(df_race_late, df_winner_list):
        df = df_race_late.groupby(['CandID'])['Term Start Year'].nunique().reset_index().rename(
            columns={'Term Start Year': 'Win at once (Late Tries)'})
        df_late = df_winner_list.merge(df, left_on='CandID', right_on='CandID', how='left')
        df_late.loc[df_late['Win at once (Late Tries)'].isnull(), 'Win at once (Late Tries)'] = 0.0
        s = df_late['Win at once (Late Tries)'].mean()
        return s

    def win_once_late_wins(df_race_late, df_winner_list):
        df = df_race_late[df_race_late['CandID'] == df_race_late['winnerID']].groupby(['CandID'])[
            'Term Start Year'].nunique().reset_index().rename(columns={'Term Start Year': 'Win at once (Late Wins)'})
        df_late_win = df_winner_list.merge(df, left_on='CandID', right_on='CandID', how='left')
        df_late_win.loc[df_late_win['Win at once (Late Wins)'].isnull(), 'Win at once (Late Wins)'] = 0.0
        s = df_late_win['Win at once (Late Wins)'].mean()
        return s

    def win_once_late_fails(df_race_late, df_winner_list):
        df = df_race_late[df_race_late['CandID'] != df_race_late['winnerID']].groupby(['CandID'])[
            'Term Start Year'].nunique().reset_index().rename(columns={'Term Start Year': 'Win at once (Late Fails)'})
        df_late_win = df_winner_list.merge(df, left_on='CandID', right_on='CandID', how='left')
        df_late_win.loc[df_late_win['Win at once (Late Fails)'].isnull(), 'Win at once (Late Fails)'] = 0.0
        s = df_late_win['Win at once (Late Fails)'].mean()
        return s

    stat_cand = dict()

    s = len(df_non_writein_id)
    print 'Number of Unique Candidates', s
    stat_cand['Number of Unique Candidates'] = len(df_non_writein_id)

    df = df_race2_all.groupby(['CandID'])['Term Start Year'].nunique().reset_index()
    s = df['Term Start Year'].mean()
    print 'Number of Election Periods Per Candidate', s
    stat_cand['Number of Election Periods Per Candidate'] = s

    cand_inc_cha_open(df_race2_all, df_non_writein_id)

    s1, s2, s3, s4, s5, df_winner_list, df_race2_all_winner, df_race2_all_loser, df_race2_all_winner_1st = win_lose(
        df_race2_all)
    print 'Number of Candidates at least winning once', s1
    stat_cand['Number of Candidates at least winning once'] = s1
    print 'Winners: Number of Winning Election Periods', s2
    stat_cand['Winners: Number of Winning Election Periods'] = s2
    print 'Winners: Number of Election Periods', s3
    stat_cand['Winners: Number of Election Periods'] = s3
    print 'Number of Candidates never win', s4
    stat_cand['Number of Candidates never win'] = s4
    print 'Losers: Number of Election Periods', s5
    stat_cand['Losers: Number of Election Periods'] = s5

    s = win_once_early_fails(df_race2_all_winner_1st, df_winner_list)
    print 'Winners: Number of Failed Tries before First Win', s
    stat_cand['Winners: Number of Failed Tries before First Win'] = s

    df_race_late = df_race2_all_winner_1st[
        df_race2_all_winner_1st['Term Start Year'] > df_race2_all_winner_1st['First Win Year']]

    s = win_once_late_tries(df_race_late, df_winner_list)
    print 'Winners: Number of Tries After First Win', s
    stat_cand['Winners: Number of Tries After First Win'] = s

    s = win_once_late_wins(df_race_late, df_winner_list)
    print 'Winners: Number of Wins After First Win', s
    stat_cand['Winners: Number of Wins After First Win'] = s

    s = win_once_late_fails(df_race_late, df_winner_list)
    print 'Winners: Number of Fails After First Win', s
    stat_cand['Winners: Number of Fails After First Win'] = s

    return stat_cand


if __name__ == '__main__':
    # ====================================================== #
    #    Initialize Directory and load Data                  #
    # ====================================================== #

    dir0 = '/Users/yuwang/Documents/research/research/timing/git'
    dir1 = dir0 + '/analysis'
    dir2 = dir0 + '/campaigns/data'
    dir3 = dir0 + '/mayors/data'
    dir4 = dir0 + '/campaigns/schema'
    dir5 = dir0 + '/mayors/schema'
    dir6 = dir0 + '/mayors'

    # create a folder for cache
    if not os.path.exists('pdata'):
        os.mkdir('pdata')
    if os.path.exists('key_race_details.csv'):
        os.remove('key_race_details.csv')
    if os.path.exists('key_race_details2.csv'):
        os.remove('key_race_details2.csv')

    key_race_details(dir3, 'Mayor', 'RaceDetails', 'key_race_details.csv')
    key_race_details(dir3, 'Mayor', 'Candidates', 'key_race_details2.csv')

    df_race = setup_race_details()
    df_race2 = setup_race_details2()

    # ====================================================== #
    #    Data Cleaning                                       #
    # ====================================================== #
    # Check if all shares in a race add up to 100
    df_shares_wrong = check_shares_sum()

    # Generate a list of unique candidates
    df_unique_CandID = unique_candidates(df_race2)

    # Remove write-in candidates, until max number of mayoral elections per candidate is reasonable
    df_non_writein = cand_remove(df_race2, ['22593', '191'])  # write-in & others
    df_non_writein_id = df_non_writein.groupby(['CandID'])['RaceID'].count().reset_index()
    print df_non_writein_id['RaceID'].describe()

    # Load the list of largest cities and merge the city names with those in ourcampaigns
    df_recent = recent_elections_city()
    df_city = city_name_merge(df_recent, df_race)

    # df_race_all is the master copy for race_details combined with recent elections
    df_race_all = race_details_recent(df_race, df_city, 'CityID', ['State', 'City'])

    # df_race2_all is the master copy for race_details, race_details2 combined with recent elections
    df_race2_all = race_details2_recent(df_non_writein, df_race_all, 'CityID')

    # df_periods is the master copy for [city, election periods]
    df_periods = df_race_all.groupby(['CityID', 'Term Start Year'])['RaceID'].count().reset_index()

    # Mark the terminal election in each election period
    df_race2_all = terminal_election(df_race2_all, 'CityID')

    # Mark the earliest election period in each city
    df_race2_all = early_city(df_race2_all, 'CityID')

    # Mark the winner in the terminal election in each election period
    df_race2_all = winner_election_period(df_race2_all, 'CityID')

    # First way of differentiating incumbent/open elections: whether name contains '(I)'
    df_race2_all = incumbent_election_v1(df_race2_all, 'CityID')

    # Second way of differentiating incumbent/open elections: whether the winner of last period appears again
    df_race2_all = incumbent_election_v2(df_race2_all, 'CityID')

    df_race2_all.to_csv('race2_all.csv')
    # ====================================================== #
    #     Summary Statistics for Cities                      #
    # ====================================================== #
    stat_city = statistics_dist(df_recent, df_city, df_periods, df_race_all, 'city', 'Cities', 'CityID')

    # ====================================================== #
    #    Summary Statistics for Elections                    #
    # ====================================================== #
    stat_election = statistics_election(df_periods, df_race2_all, 'CityID')

    # ====================================================== #
    #    Summary Statistics for Candidates                   #
    # ====================================================== #

    # df_race2_all, df_non_writein_id = select_districts(df_race2_all, 'CityID', 100, 'Term Start Year', 1950)
    stat_cand = statistics_candidates()
