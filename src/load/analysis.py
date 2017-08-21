import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from stat_data import *
from df2tex import df2tex


if __name__ == '__main__':
    lookup_office = {'CityID': 'Mayor', 'StateID': 'Governor'}
    distID = 'StateID'

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
