import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from stat_data import *
from df2tex import df2tex


if __name__ == '__main__':
    lookup_office = {'CityID': 'Mayor', 'StateID': 'Governor'}
    for distID in ['CityID']:
        df_all = pd.read_pickle("../../data/pdata/df_all_{}.pkl".format(distID))

        df_all = select_dist(df_all, [[distID, 1000]], [['Term Start', datetime(1950, 1, 1)]])
        stat_dist = statistics_dist(df_all, distID)
        stat_election = statistics_election(df_all, distID)

        df_all = df_all[df_all['First Try'] >= df_all['Earlist Date'] + timedelta(days=100)]
        stat_cand = statistics_candidates(df_all)

        row_name = [
            ['stat_dist', 'N', 'N with Data', 'Avg Ranks (Unweighted)', 'Avg Ranks (Weighted by Periods)',
             'Avg Election Periods', 'Avg Elections', 'Avg Term Lengths'],
            ['stat_election', 'Elections Covered', 'Election Periods Covered', 'Incumbent Election Periods',
             'Incumbent Election Candidates', 'Open Election Periods', 'Open Election Candidates',
             'Unclear Election Periods', 'Unclear Election Candidates'],
            ['stat_cand', 'Number of Unique Candidates', 'Number of Election Periods Per Candidate',
             'Number of Candidates at least winning once', 'Number of Candidates never win',
             'Winners: Number of Election Periods', 'Winners: Number of Winning Election Periods',
             'Winners: Number of Failed Tries before First Win', 'Winners: Number of Tries After First Win',
             'Winners: Number of Wins After First Win', 'Winners: Number of Fails After First Win',
             'Winners: Number of First Win is Challenger','Winners: Number of First Win is Open',
             'Winners: Number of First Win is Unclear','Losers: Number of Election Periods',
             'Losers: Number of Challenger Periods','Losers: Number of Open Periods',
             'Losers: Number of Unclear Periods']
        ]
        for index, stats in enumerate([stat_dist, stat_election, stat_cand]):
            row_head = row_name[index][0]
            row_tail = row_name[index][1:]
            df = pd.DataFrame({'Variable': stats.keys(), 'Value': stats.values()}).set_index('Variable', drop=False)
            df = df.reindex(row_tail)
            print df
            df2tex(df, '/Users/yuwang/Dropbox/local politicians/model/analysis_{}/'.format(lookup_office[distID]),
                   '{}.tex'.format(row_head), "%8.2f", 0, ['Value'], ['Variable'], ['Variable', 'Value'])


    # Step 1: Construct a list of non-mayoral elections
    df_City = df_all #pd.read_pickle("../../data/pdata/df_all_CityID.pkl")
    df_All = pd.read_pickle("../../data/pdata/df_all_AllID.pkl")
    df_All_RaceID = pd.DataFrame(df_All['RaceID'].unique()).rename(columns={0:'RaceID'})
    df_City_RaceID = pd.DataFrame(df_City['RaceID'].unique()).rename(columns={0:'RaceID'})
    df_common_RaceID = df_All_RaceID.merge(df_City_RaceID, left_on='RaceID',right_on='RaceID')
    df_non_City_RaceID = df_All_RaceID[~df_All_RaceID['RaceID'].isin(df_common_RaceID['RaceID'])]
    print df_non_City_RaceID['RaceID'].nunique()
    df_non_City = df_All.merge(df_non_City_RaceID, on=['RaceID'], how='right').reset_index().rename(columns={'RaceID_x':'RaceID'})
    print df_non_City['CandID'].nunique()

    # Step 2: Construct a list of non-mayoral elections associated with each mayoral candidate
    df_City_CandID = pd.DataFrame(df_City['CandID'].unique()).rename(columns={0:'CandID'})
    df_non_City['CandID'] = df_non_City['CandID'].astype(float)
    dict_mates_RaceID = dict()
    dict_mates_CandID = dict()
    list_mates_CandID = list()
    for index, row in df_City_CandID.iterrows():
        df_race = df_non_City[df_non_City['CandID']==row[0]]
        race_list = df_race['RaceID'].unique().tolist()
        dict_mates_RaceID['{}'.format(row[0])] = race_list
        df_cand = df_non_City[df_non_City['RaceID'].isin(race_list)]
        cand_list = df_cand['CandID'].unique().tolist()
        if row[0] in cand_list:
            cand_list.remove(row[0])
        dict_mates_CandID['{}'.format(row[0])] = cand_list
        list_mates_CandID.extend(cand_list)
    df_mates_CandID = pd.DataFrame(list_mates_CandID).rename(columns={0:'CandID'})
    print df_mates_CandID['CandID'].nunique()

    # Step 3: How many election mates run for mayoral elections themselves?
    df_common = df_mates_CandID.merge(df_City_CandID,on=['CandID']).reset_index().rename(columns={'CandID_x':'CandID'})
    print df_common['CandID'].nunique()

    # Step 4: Generate message:
    print 'Out of', df_mates_CandID['CandID'].nunique(), 'candidates who have run for non-mayoral races with mayoral candidates,', df_common['CandID'].nunique(), 'have run for mayors.'
    print 'There are in total',df_City['CandID'].nunique(),'mayoral candidates.'




