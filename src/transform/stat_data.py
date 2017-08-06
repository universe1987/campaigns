import os
import re
import csv
import json
import time
import collections
import numpy as np
import pandas as pd

def statistics_dist(df_recent, df_dist, df_periods, df_race_all, dist, dists, distID):
    stat_dist = dict()

    s = len(df_recent[dist])
    print 'Total {}'.format(dists), s
    stat_dist['Total {}'.format(dists)] = s

    if dist == 'city':
       s = len(df_recent[df_recent['web'].str.contains('http')])
       print 'Total {} with Data'.format(dists), s
       stat_dist['Total {} with Data'.format(dists)] = s

    df_dist[distID] = df_dist[distID].astype(float)
    s = df_dist[distID].mean()
    print 'Avg Ranks', s
    stat_dist['Avg Ranks'] = s

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
    df = df.groupby([distID,'Term Start Year'])['RaceID'].max().reset_index()
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
    df_non_writein_id = df_race2_all.groupby(['CandID'])['RaceID'].count().reset_index().rename(columns={'RaceID': 'RaceIDs'})
    return df_race2_all, df_non_writein_id

def statistics_candidates(df_race2_all, df_non_writein_id):
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

        return s1,s2,s3,s4,s5,df_winner_list, df_race2_all_winner, df_race2_all_loser, df_race2_all_winner_1st

    def win_once_early_fails(df_race2_all_winner_1st, df_winner_list):
        df = df_race2_all_winner_1st[df_race2_all_winner_1st['Term Start Year'] < df_race2_all_winner_1st['First Win Year']]
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

    s = df_non_writein_id['CandID'].nunique()
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