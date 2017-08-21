import pandas as pd
from datetime import datetime, timedelta

def select_dist(df, key_less_cutoff,key_greater_cutoff):
    print 'pre-selection',len(df)
    for items in key_less_cutoff:
        df = df[df[items[0]] <= items[1]]
    for items in key_greater_cutoff:
        df = df[df[items[0]] >= items[1]]
    print 'post-selection',len(df)
    return df


def statistics_dist(df_race_all, distID):
    lookup = {'CityID':['city','City','Cities'],'StateID':['state','State','States']}
    dist, distC, distC_plural = [lookup[distID][0],lookup[distID][1],lookup[distID][2]]

    stat_dist = dict()

    df_recent = pd.read_csv('../../data/entry/recent_elections_{}.csv'.format(dist),usecols=[0],names=['Entry'])
    df_recent.loc[:,'Entry'] = df_recent['Entry'].astype(str)
    df_recent[distID] = range(len(df_recent))
    df_data = df_recent[df_recent['Entry'].str.contains('\d+')]

    stat_dist['N'] = len(df_recent['Entry'])
    stat_dist['N with Data'] = len(df_data)
    stat_dist['Avg Ranks (Unweighted)'] = df_data[distID].mean()

    df_period = df_race_all.groupby([distID, 'Term Start'])['RaceID'].count().reset_index()
    stat_dist['Avg Ranks (Weighted by Periods)'] = df_period[distID].mean()

    df = df_race_all.groupby([distID])['Term Start'].nunique().reset_index()
    stat_dist['Avg Election Periods'] = df['Term Start'].mean()

    df = df_race_all.groupby([distID])['RaceID'].nunique().reset_index()
    stat_dist['Avg Elections'] = df['RaceID'].mean()

    df_race_all.loc[:,'Term Length'] = df_race_all['Term Length'].astype(str).str.extract('(\d+)').astype(float)/365
    stat_dist['Avg Term Lengths'] = df_race_all.groupby([distID,'Term Start'])['Term Length'].max().mean()

    print stat_dist
    return stat_dist

def statistics_election(df_race2_all, distID):
    stat_election = dict()

    stat_election['Elections Covered'] = df_race2_all['RaceID'].nunique()
    stat_election['Election Periods Covered'] = len(df_race2_all.groupby([distID,'Term Start'])['RaceID'].count())

    lookup = {1:'Incumbent',0:'Open',2:'Unclear'}
    for label, value in lookup.iteritems():
        df_type = df_race2_all[df_race2_all['Incumbent2'] == label]
        stat_election['{} Election Periods'.format(value)] = len(df_type.groupby([distID, 'Term Start'])['RaceID'].count())
        stat_election['{} Election Candidates'.format(value)] = df_type['CandID'].nunique()

    print stat_election
    return stat_election



def statistics_candidates(df_race2_all):

    def cand_inc_cha_open(df_race2_all):
        dict_stat=dict()
        dic0 = {'elections': 'RaceID', 'election periods': 'Term Start'}
        dic1 = {'Open': df_race2_all['Incumbent2'] == 0,
                'Incumbent': (df_race2_all['Incumbent2'] == 1) & (
                    df_race2_all['CandID'] == df_race2_all['winnerID previous']),
                'Challenger': (df_race2_all['Incumbent2'] == 1) & (
                    df_race2_all['CandID'] != df_race2_all['winnerID previous']),
                'Unclear': df_race2_all['Incumbent2'] == 2}
        df_ID_list = pd.DataFrame(df_race2_all['CandID'].unique(),columns=['CandID'])
        for key0, value0 in dic0.iteritems():
            for key1, value1 in dic1.iteritems():
                df = df_race2_all[value1].groupby(['CandID'])[value0].nunique().reset_index()\
                    .rename(columns={value0: '{} {}'.format(key1, key0)})
                df2 = df_ID_list.merge(df, left_on='CandID', right_on='CandID', how='outer')
                df2.loc[df2['{} {}'.format(key1, key0)].isnull(), '{} {}'.format(key1, key0)] = 0
                dict_stat['Avg number of {} {}'.format(key1, key0)] = df2['{} {}'.format(key1, key0)].mean()
        return dict_stat

    def tries_before_after(df_race2_all):
        dict_stat = dict()

        df_winner_all = df_race2_all[df_race2_all['winner ever']==1.0]
        df_winner_win = df_winner_all[~df_winner_all['winnerID'].isnull()]
        df_winner_lose = df_winner_all[df_winner_all['winnerID'].isnull()]
        df_winner_list = pd.DataFrame(df_winner_all['CandID'].unique(),columns=['CandID'])
        df_winner_first = df_winner_all[df_winner_all['Term Start']==df_winner_all['First Win']]
        df_loser_all = df_race2_all[df_race2_all['winner ever'] == 0.0]

        dict_stat['Number of Unique Candidates'] = len(df_race2_all['CandID'].unique())
        df = df_race2_all.groupby(['CandID'])['Term Start'].nunique().reset_index()
        dict_stat['Number of Election Periods Per Candidate'] = df['Term Start'].mean()
        dict_stat['Number of Candidates at least winning once'] = len(df_winner_list)
        dict_stat['Winners: Number of Winning Election Periods'] = df_winner_win.groupby(['CandID'])['Term Start'].nunique().mean()
        dict_stat['Winners: Number of Election Periods'] = df_winner_all.groupby(['CandID'])['Term Start'].nunique().mean()
        df = df_winner_first[df_winner_first['Incumbent2']==1].groupby(['CandID'])['Term Start'].nunique().reset_index()
        dict_stat['Winners: Number of First Win is Challenger']= df['Term Start'].sum()
        df = df_winner_first[df_winner_first['Incumbent2']==0].groupby(['CandID'])['Term Start'].nunique().reset_index()
        dict_stat['Winners: Number of First Win is Open'] = df['Term Start'].sum()
        df = df_winner_first[df_winner_first['Incumbent2'] == 2].groupby(['CandID'])['Term Start'].nunique().reset_index()
        dict_stat['Winners: Number of First Win is Unclear'] = df['Term Start'].sum()
        dict_stat['Number of Candidates never win'] = len(df_loser_all['CandID'].unique())
        dict_stat['Losers: Number of Election Periods'] = df_loser_all.groupby(['CandID'])['Term Start'].nunique().mean()
        df = df_loser_all[df_loser_all['Incumbent2'] == 1].groupby(['CandID'])['Term Start'].nunique().reset_index()
        dict_stat['Losers: Number of Challenger Periods']= df['Term Start'].sum()
        df = df_loser_all[df_loser_all['Incumbent2'] == 0].groupby(['CandID'])['Term Start'].nunique().reset_index()
        dict_stat['Losers: Number of Open Periods']= df['Term Start'].sum()
        df = df_loser_all[df_loser_all['Incumbent2'] == 2].groupby(['CandID'])['Term Start'].nunique().reset_index()
        dict_stat['Losers: Number of Unclear Periods'] = df['Term Start'].sum()


        df_n_before_win = df_winner_all[df_winner_all['Term Start'] < df_winner_all['First Win']].groupby(['CandID'])['Term Start'].nunique().reset_index()
        df_n_after_win = df_winner_all[df_winner_all['Term Start'] > df_winner_all['First Win']].groupby(['CandID'])[
            'Term Start'].nunique().reset_index()
        df_n_win_after_win = df_winner_win[df_winner_win['Term Start'] > df_winner_win['First Win']].groupby(['CandID'])[
            'Term Start'].nunique().reset_index()
        df_n_fail_after_win = df_winner_lose[df_winner_lose['Term Start'] > df_winner_lose['First Win']].groupby(['CandID'])[
            'Term Start'].nunique().reset_index()

        s_vals = []
        for df_n in [df_n_before_win, df_n_after_win, df_n_win_after_win, df_n_fail_after_win]:
            df = df_n.merge(df_winner_list,left_on='CandID',right_on='CandID',how='outer')
            df.loc[df['Term Start'].isnull(),'Term Start']=0
            s_vals.append(df['Term Start'].mean())

        lookup = {0:'Winners: Number of Failed Tries before First Win',1: 'Winners: Number of Tries After First Win',
                  2:'Winners: Number of Wins After First Win', 3: 'Winners: Number of Fails After First Win'}
        for key, value in lookup.iteritems():
            dict_stat[value] = s_vals[key]

        return dict_stat


    stat_cand = (cand_inc_cha_open(df_race2_all))
    stat_cand.update(tries_before_after(df_race2_all))
    print 'stat_cand', stat_cand

    return stat_cand