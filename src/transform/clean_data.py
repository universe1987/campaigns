from datetime import datetime, timedelta


def race_details_recent(df_race, df_dist, distID, label):
    df_race_all = df_race.merge(df_dist, left_on=label, right_on=label, how='outer')
    df_race_distID = df_race_all.groupby([distID])['RaceID'].count().reset_index()
    print df_race_distID['RaceID'].describe()
    return df_race_all


def race_details2_recent(df_non_writein, df_race_all, distID):
    df_non_writein.loc[:, 'RaceID'] = df_non_writein['RaceID'].astype(float).astype(str)
    df_race_all.loc[:, 'RaceID'] = df_race_all['RaceID'].astype(float).astype(str)
    df_all = df_non_writein.merge(df_race_all, left_on=['RaceID'], right_on=['RaceID'], how='outer')
    df_race2_distID = df_all.groupby([distID])['CandID'].count().reset_index()
    print df_race2_distID['CandID'].describe()
    return df_all


def term_start_merge(df_all, distID, cutoff):
    df_sort = df_all.sort_values([distID, 'Term Start'], ascending=True)
    count = 0
    while True:
        df_sort['Term Start Next'] = df_sort.groupby([distID])['Term Start'].shift(-1)
        df_sort['Term Start Diff'] = df_sort['Term Start Next'] - df_sort['Term Start']
        df_sort.loc[df_sort['Term Start Diff'] < cutoff, 'Term Start'] = df_sort['Term Start Next']
        df_sort.loc[df_sort['Term Start Diff'] <= timedelta(days=0), 'Term Start Diff'] = cutoff + timedelta(days=2000)
        s = df_sort['Term Start Diff'].min()
        count = count + 1
        # print 's', s, count
        if s > cutoff:
            break
    df_all = df_sort
    return df_all


def terminal_election(df_all, distID):
    df_t = df_all.groupby([distID, 'Term Start'])['Polls Close'].max().reset_index().rename(
        columns={'Polls Close': 'Terminal Date'})
    df_all = df_all.merge(df_t, left_on=[distID, 'Term Start'], right_on=[distID, 'Term Start'])
    df_all['Terminal'] = (df_all['Polls Close'] == df_all['Terminal Date'])
    return df_all


def early_dist(df_all, distID):
    df_e = df_all.groupby([distID])['Term Start'].min().reset_index().rename(columns={'Term Start': 'Earlist Date'})
    df_all = df_all.merge(df_e, left_on=[distID], right_on=[distID])
    df_all['Earlist'] = (df_all['Term Start'] == df_all['Earlist Date'])
    return df_all


def winner_follower(df_all, df_in, distID, rank):  # rank='winner', rankID='winnerID'
    rankID = rank + 'ID'
    df0 = df_in.groupby([distID, 'Term Start'])['Votes'].max().reset_index().rename(columns={'Votes': 'Votes Max'})
    df1 = df_all.merge(df0, left_on=[distID, 'Term Start'], right_on=[distID, 'Term Start'])
    df1[rank] = (df1['Votes'] == df1['Votes Max'])
    df_list = df1.groupby([distID, 'Term Start', 'CandID'])[rank].max().reset_index()
    df_all = df_all.merge(df_list, left_on=[distID, 'Term Start', 'CandID'], right_on=[distID, 'Term Start', 'CandID'],
                          how='outer')
    df_all[rankID] = df_all[rank] * df_all['CandID'].astype(float)
    df_all.loc[df_all[rankID] == 0.0, rankID] = None
    df_all[rank] = df_all[rank].astype(bool)
    return df_all


def key_election(df_all, distID):
    df_k1 = df_all.groupby([distID, 'Term Start', 'RaceID'])['Votes'].sum().reset_index()
    df_k2 = df_k1.groupby([distID, 'Term Start'])['Votes'].max().reset_index().rename(
        columns={'Votes': 'Votes Sum Max'})
    df_k3 = df_k1.merge(df_k2, left_on=[distID, 'Term Start'], right_on=[distID, 'Term Start'])
    df_k3['KeyRace'] = (df_k3['Votes'] == df_k3['Votes Sum Max'])
    df_k3['KeyRaceID'] = df_k3['KeyRace'] * df_k3['RaceID'].astype(float)
    df_k3 = df_k3[['KeyRaceID', 'Votes Sum Max', 'KeyRace', 'RaceID']]
    df_all = df_all.merge(df_k3, left_on='RaceID', right_on='RaceID')
    return df_all


def win_follow_ever(df_all):
    df = df_all.groupby(['CandID'])['winner', 'follower', 'winner_key', 'follower_key'].max().reset_index() \
        .rename(columns={'winner': 'winner ever', 'follower': 'follower ever', 'winner_key': 'winner_key ever',
                         'follower_key': 'follower_key ever'})
    df_all = df_all.merge(df, left_on='CandID', right_on='CandID')
    return df_all


def incumbent_election_v1(df_all, distID):
    df_all['Name Flag'] = df_all['Name'].str.startswith('(I)') | df_all['Name'].str.contains('Mayor')
    df = df_all.groupby([distID, 'Term Start'])['Name Flag'].max().reset_index()
    df['Incumbent1'] = df['Name Flag']
    df_all = df_all.merge(df, left_on=[distID, 'Term Start'], right_on=[distID, 'Term Start'], how='outer')
    return df_all


def incumbent_election_v2(df_all, distID):
    df_winner_id = df_all[['winnerID', distID, 'Term Start']] \
        .groupby([distID, 'Term Start'])['winnerID'].max().reset_index() \
        .sort_values([distID, 'Term Start'], ascending=True)
    df_winner_id['winnerID previous'] = df_winner_id.groupby([distID])['winnerID'].shift(1)
    df_winner_id = df_winner_id.drop('winnerID', 1)

    df_all = df_all.merge(df_winner_id, left_on=[distID, 'Term Start'],
                          right_on=[distID, 'Term Start'], how='outer')
    df_all.loc[:, 'CandID'] = df_all['CandID'].astype(float)
    df_all.loc[:, 'winnerID previous'] = df_all['winnerID previous'].astype(float)
    df_all['Matched'] = df_all['CandID'] == df_all['winnerID previous']

    df = df_all.groupby([distID, 'Term Start'])['Matched'].max().reset_index()
    df = df.rename(columns={'Matched': 'Incumbent2'})
    df['Incumbent2'] = df['Incumbent2'].astype(int)
    df_all = df_all.merge(df, left_on=[distID, 'Term Start'], right_on=[distID, 'Term Start'], how='outer')
    df_all.loc[df_all['Incumbent2'] != 1, 'Incumbent2'] = df_all['Earlist'] * 2
    # Incumbent2 = {1:incumbent, 0:open, 2:unclear}
    return df_all


def career_span(df_all):
    df1 = df_all.groupby(['CandID'])['Term Start'].agg([min, max]).reset_index().rename(
        columns={'min': 'First Try', 'max': 'Last Try'})
    df_all = df_all.merge(df1, left_on='CandID', right_on='CandID')
    return df_all


def first_win(df_all):
    df = df_all.groupby('winnerID')['Term Start'].min().reset_index().rename(
        columns={'winnerID': 'CandID', 'Term Start': 'First Win'})
    df_all = df_all.merge(df, left_on='CandID', right_on='CandID', how='outer')
    return df_all
