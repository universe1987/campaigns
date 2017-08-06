import os
import re
import csv
import json
import time
import collections
import numpy as np
import pandas as pd

def race_details_recent(df_race, df_dist, distID, label):
    df_race_all = df_race.merge(df_dist, left_on = label, right_on = label, how = 'outer')
    df_race_distID = df_race_all.groupby([distID])['RaceID'].count().reset_index()
    print df_race_distID['RaceID'].describe()
    return df_race_all

def race_details2_recent(df_non_writein, df_race_all, distID):
    df_non_writein.loc[:, 'RaceID'] = df_non_writein['RaceID'].astype(float).astype(str)
    df_race_all.loc[:, 'RaceID'] = df_race_all['RaceID'].astype(float).astype(str)
    df_race2_all = df_non_writein.merge(df_race_all, left_on = ['RaceID'], right_on = ['RaceID'], how = 'outer')
    df_race2_distID = df_race2_all.groupby([distID])['CandID'].count().reset_index()
    print df_race2_distID['CandID'].describe()
    return df_race2_all

def terminal_election(df_race2_all, distID):
    df_terminal = df_race2_all.groupby([distID, 'Term Start Year'])['Polls Close_date'].max().reset_index()\
                  .rename(columns={'Polls Close_date': 'Terminal Date'})
    df_race2_all = df_race2_all.merge(df_terminal, left_on=[distID, 'Term Start Year'],right_on=[distID, 'Term Start Year'], how='outer')
    df_race2_all['Terminal'] = (df_race2_all['Polls Close_date'] == df_race2_all['Terminal Date'])
    return df_race2_all

def early_dist(df_race2_all, distID):
    df_early = df_race2_all.groupby([distID])['Term Start Year'].min().reset_index()\
               .rename(columns={'Term Start Year': 'Earlist Date'})
    df_race2_all = df_race2_all.merge(df_early, left_on=[distID], right_on=[distID], how='outer')
    df_race2_all['Earlist'] = (df_race2_all['Term Start Year'] == df_race2_all['Earlist Date'])
    return df_race2_all

def winner_election_period(df_race2_all, distID):
    df_winner = df_race2_all[df_race2_all['Terminal'] == True]
    df_winner = df_winner.groupby([distID, 'Term Start Year'])['Votes'].max().reset_index()\
                .rename(columns={'Votes': 'Votes Max Election Period'})
    df_race2_all = df_race2_all.merge(df_winner, left_on=[distID, 'Term Start Year'],right_on=[distID, 'Term Start Year'], how='outer')
    df_race2_all['winner'] = (df_race2_all['Votes'] == df_race2_all['Votes Max Election Period']) * 1.0
    df_race2_all['winnerID'] = df_race2_all['winner'] * df_race2_all['CandID'].astype(float)
    return df_race2_all

def follower_election_period(df_race2_all, distID):
    df_follower = df_race2_all[(df_race2_all['Terminal'] == True) & (df_race2_all['winner'] == False)]
    df_follower = df_follower.groupby([distID,'Term Start Year'])['Votes'].max().reset_index()\
                  .rename(columns = {'Votes':'Votes 2nd Election Period'})
    df_follower = df_follower[df_follower['Votes 2nd Election Period'].str.contains(r'\d+')]
    df_race2_all = df_race2_all.merge(df_follower, left_on=[distID, 'Term Start Year'], right_on=[distID, 'Term Start Year'], how='outer')
    df_race2_all['follower'] = (df_race2_all['Votes']==df_race2_all['Votes 2nd Election Period']) * 1.0
    df_race2_all['followerID'] = df_race2_all['follower'] * df_race2_all['CandID'].astype(float)
    return df_race2_all

def key_election(df_race2_all, distID):
    df_race2_all.loc[:,'Votes']=df_race2_all['Votes'].str.replace(",",'').astype(float)
    df_key1 = df_race2_all.groupby([distID,'Term Start Year','RaceID'])['Votes'].sum().reset_index()
    df_key2 = df_key1.groupby([distID, 'Term Start Year'])['Votes'].max().reset_index().rename(columns={'Votes':'Votes Sum Max'})
    df_key3 = df_key1.merge(df_key2,left_on=[distID,'Term Start Year'],right_on=[distID,'Term Start Year'],how='outer')
    df_key3['KeyRace'] = (df_key3['Votes']==df_key3['Votes Sum Max'])
    df_key3['KeyRaceID'] = (df_key3['Votes']==df_key3['Votes Sum Max'])*1.0*df_key3['RaceID'].astype(float)
    df_key3 = df_key3[['KeyRaceID','Votes Sum Max','KeyRace','RaceID']]
    df_race2_all = df_race2_all.merge(df_key3,left_on='RaceID',right_on='RaceID',how='outer')
    return df_race2_all

def winner_key_election(df_race2_all, distID):
    df_winner = df_race2_all[df_race2_all['KeyRace'] == True]
    df_winner = df_winner.groupby([distID, 'Term Start Year'])['Votes'].max().reset_index()\
                .rename(columns={'Votes': 'Votes Max Key Race'})
    df_race2_all = df_race2_all.merge(df_winner, left_on=[distID, 'Term Start Year'],right_on=[distID, 'Term Start Year'], how='outer')
    df_race2_all['winner_key'] = (df_race2_all['Votes'] == df_race2_all['Votes Max Key Race']) * 1.0
    df_race2_all['winner_keyID'] = df_race2_all['winner_key'] * df_race2_all['CandID'].astype(float)
    return df_race2_all

def follower_key_election(df_race2_all, distID):
    df_follower = df_race2_all[(df_race2_all['KeyRace'] == True) & (df_race2_all['winner_key'] == False)]
    df_follower = df_follower.groupby([distID,'Term Start Year'])['Votes'].max().reset_index()\
                  .rename(columns = {'Votes':'Votes 2nd Key Race'})
    df_follower.loc[:,'Votes 2nd Key Race'] = df_follower['Votes 2nd Key Race'].astype(str)
    df_follower = df_follower[df_follower['Votes 2nd Key Race'].str.contains(r'\d+')]
    df_race2_all = df_race2_all.merge(df_follower, left_on=[distID, 'Term Start Year'], right_on=[distID, 'Term Start Year'], how='outer')
    df_race2_all['follower_key'] = (df_race2_all['Votes']==df_race2_all['Votes 2nd Key Race'])*1.0
    df_race2_all['follower_keyID'] = df_race2_all['follower_key'] * df_race2_all['CandID'].astype(float)
    return df_race2_all

def win_follow_ever(df_race2_all):
    df = df_race2_all.groupby(['CandID'])['winner','follower','winner_key','follower_key'].max().reset_index()\
         .rename(columns={'winner':'winner ever','follower':'follower ever','winner_key':'winner_key ever','follower_key':'follower_key ever'})
    df_race2_all = df_race2_all.merge(df,left_on='CandID',right_on='CandID',how='outer')
    return df_race2_all

def incumbent_election_v1(df_race2_all, distID):
    df_race2_all['Name Flag'] = (df_race2_all['Name'].str.contains('I')) * 1.0
    df = df_race2_all.groupby([distID, 'Term Start Year'])['Name Flag'].sum().reset_index()
    df['Incumbent1'] = (df['Name Flag'] > 0.0) * 1.0
    df_race2_all = df_race2_all.merge(df, left_on=[distID, 'Term Start Year'], right_on=[distID, 'Term Start Year'], how='outer')
    return df_race2_all

def incumbent_election_v2(df_race2_all, distID):
    df_winner_id = df_race2_all[['winnerID', distID, 'Term Start Year']]\
                   .groupby([distID, 'Term Start Year'])['winnerID'].max().reset_index()\
                   .sort_values([distID, 'Term Start Year'], ascending=True)
    df_winner_id['winnerID previous'] = df_winner_id.groupby([distID])['winnerID'].shift(1)
    df_winner_id = df_winner_id.drop('winnerID', 1)

    df_race2_all = df_race2_all.merge(df_winner_id, left_on=[distID, 'Term Start Year'],
                                      right_on=[distID, 'Term Start Year'], how='outer')
    df_race2_all['CandID'] = df_race2_all['CandID'].astype(float)
    df_race2_all['winnerID previous'] = df_race2_all['winnerID previous'].astype(float)
    df_race2_all['Matched'] = (df_race2_all['CandID'] == df_race2_all['winnerID previous']) * 1.0

    df = df_race2_all.groupby([distID,'Term Start Year'])['Matched'].max().reset_index()
    df = df[df['Matched'] > 0]
    df = df.rename(columns = {'Matched':'Incumbent2'})
    df_race2_all = df_race2_all.merge(df, left_on = [distID,'Term Start Year'], right_on = [distID, 'Term Start Year'], how = 'outer')
    df_race2_all.loc[df_race2_all['Incumbent2'] != 1,'Incumbent2'] = (df_race2_all['Earlist'])* 2.0
    # Incumbent2 = {1:incumbent, 0:open, 2:unclear}

    return df_race2_all

def career_span(df_race2_all):
    df1 = df_race2_all.groupby(['CandID'])['Term Start Year'].agg([min, max]).reset_index().rename(columns={'min':'Career Start Year','max':'Career End Year'})
    df_race2_all = df_race2_all.merge(df1, left_on='CandID', right_on='CandID',how='outer')
    return df_race2_all

