import numpy as np
import pandas as pd
import os, re, csv,json, time, collections
from load_data import key_race_details,check_shares_sum,\
                      setup_race_details, setup_race_details2,unique_candidates,\
                      cand_remove, recent_elections, dist_name_merge
from clean_data import race_details_recent, race_details2_recent,terminal_election, early_dist, winner_election_period, \
                       follower_election_period, key_election, winner_key_election, follower_key_election, \
                       win_follow_ever, incumbent_election_v1, incumbent_election_v2, career_span
from stat_data import statistics_dist,statistics_election, statistics_candidates, select_districts
from export import dict2tex2


if __name__ == '__main__':
    # ====================================================== #
    #    Initialize Directory and load Data                  #
    # ====================================================== #

    folder = '../../data/json'


    # Create a dictionary for governor and mayor
    key_race_details(folder, 'Governor', 'RaceDetails', 'key_race_details.csv')
    key_race_details(folder, 'Governor', 'Candidates', 'key_race_details2.csv')

    # df_race = setup_race_details(dicts[1])
    # df_race2 = setup_race_details2()
    #
    # # Check if all shares in a race add up to 100
    # df_shares_wrong = check_shares_sum(df_race2, df_race)
    #
    # # Generate a list of unique candidates
    # df_unique_CandID = unique_candidates(df_race2)
    #
    # # Remove write-in candidates, until max number of mayoral elections per candidate is reasonable
    # df_non_writein = cand_remove(df_race2, ['22593', '191', '19359', '30530', '4666'])  # write-in & others ,'4667'
    # df_non_writein_id = df_non_writein.groupby(['CandID'])['RaceID'].count().reset_index()
    # df_non_writein_id = df_non_writein_id.sort_values(['RaceID'], ascending=True)
    # print df_non_writein_id['RaceID'].describe()
    #
    # # Load the list of largest cities and merge the city names with those in ourcampaigns
    # df_recent = recent_elections(dicts[1], dir6, dir7)
    # df_dist = dist_name_merge(df_recent, df_race, dicts[1])
    #
    # # ====================================================== #
    # #    Data Cleaning                                       #
    # # ====================================================== #
    # # df_race_all is the master copy for race_details combined with recent elections
    # df_race_all = race_details_recent(df_race, df_dist, dicts[2], dicts[5])
    #
    # # df_race2_all is the master copy for race_details, race_details2 combined with recent elections
    # df_race2_all = race_details2_recent(df_non_writein, df_race_all, dicts[2])
    #
    # # df_periods is the master copy for [city, election periods]
    # df_periods = df_race_all.groupby([dicts[2], 'Term Start Year'])['RaceID'].count().reset_index()
    #
    # # Mark the terminal election in each election period
    # df_race2_all = terminal_election(df_race2_all, dicts[2])
    #
    # # Mark the earliest election period in each city
    # df_race2_all = early_dist(df_race2_all,dicts[2])
    #
    # # Mark the winner in the terminal election in each election period
    # df_race2_all = winner_election_period(df_race2_all,dicts[2])
    #
    # # Mark the follower (2nd place runner) in the terminal election in each election period
    # df_race2_all = follower_election_period(df_race2_all,dicts[2])
    #
    # # Mark the key election in each election period
    # df_race2_all = key_election(df_race2_all, dicts[2])
    #
    # # Mark the winner in the key election in each election period
    # df_race2_all = winner_key_election(df_race2_all, dicts[2])
    #
    # # Mark the follower (2nd place runner) in the key election in each election period
    # df_race2_all = follower_key_election(df_race2_all, dicts[2])
    #
    # # Mark the candidates who have ever made to mayor position
    # df_race2_all = win_follow_ever(df_race2_all)
    #
    # # First way of differentiating incumbent/open elections: whether name contains '(I)'
    # df_race2_all = incumbent_election_v1(df_race2_all, dicts[2])
    #
    # # Second way of differentiating incumbent/open elections: whether the winner of last period appears again
    # df_race2_all = incumbent_election_v2(df_race2_all, dicts[2])
    #
    # # Mark the earliest and latest year of race for each candidate
    # df_race2_all = career_span(df_race2_all)
    #
    # # Mark the candidate searched by Sam Gerson
    # #df_race2_all = sam(df_race2_all)
    # #df_race2_all = sam_source(df_race2_all)
    #
    # #df_race2_all.to_csv('pdata/race2_all.csv')
    # # ====================================================== #
    # #     Summary Statistics for Cities                      #
    # # ====================================================== #
    # stat_dist = statistics_dist(df_recent, df_dist, df_periods, df_race_all, dicts[3],dicts[4],dicts[2])
    #
    # # ====================================================== #
    # #    Summary Statistics for Elections                    #
    # # ====================================================== #
    # stat_election = statistics_election(df_periods, df_race2_all, dicts[2])
    #
    # # ====================================================== #
    # #    Summary Statistics for Candidates                   #
    # # ====================================================== #
    #
    # #df_race2_all, df_non_writein_id = select_districts(df_race2_all, 'CityID', 100, 'Term Start Year', 1950)
    # stat_cand = statistics_candidates(df_race2_all, df_non_writein_id)
    #
    # # ====================================================== #
    # #    List of Names for RA                                #
    # # ====================================================== #
    # #df_name_RA = RA_name_list(df_race2_all)
    #
    # # ====================================================== #
    # #    Export to Latex Tables                              #
    # # ====================================================== #
    # def f1(x):
    #     return '%13.2f' % x
    #
    # dic = {'stat_dist': ['Total {}'.format(dicts[4]), 'Total {} with Data'.format(dicts[4]), 'Avg Ranks', 'Median Ranks',
    #                 'Avg Election Periods','Avg Elections', 'Avg Term Lengths'],
    #        'stat_election': ['Election Covered', 'Election Periods Covered', 'Incumbent Election Periods',
    #                     'Incumbent Election Candidates','Open Election Periods', 'Open Election Candidates',
    #                     'Unclear Election Periods','Unclear Election Candidates'],
    #        'stat_cand': ['Number of Unique Candidates', 'Number of Election Periods Per Candidate',
    #                 'Number of Candidates at least winning once','Winners: Number of Winning Election Periods',
    #                 'Winners: Number of Election Periods', 'Number of Candidates never win',
    #                 'Losers: Number of Election Periods','Winners: Number of Failed Tries before First Win',
    #                 'Winners: Number of Tries After First Win', 'Winners: Number of Wins After First Win',
    #                 'Winners: Number of Fails After First Win']
    #        }

    #for key, value in dic.iteritems():
    #    dict2tex2(key,['Variable','Value'], value, dicts[7]+'/{}.tex'.format(key), [None, f1])

