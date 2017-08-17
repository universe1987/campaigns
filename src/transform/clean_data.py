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
    df_t = df_race2_all.groupby([distID, 'Term Start'])['Polls Close'].max().reset_index()\
                  .rename(columns={'Polls Close': 'Terminal Date'})
    df_race2_all = df_race2_all.merge(df_t, left_on=[distID, 'Term Start'],right_on=[distID, 'Term Start'], how='outer')
    df_race2_all['Terminal'] = (df_race2_all['Polls Close'] == df_race2_all['Terminal Date'])*1.0
    return df_race2_all

def early_dist(df_race2_all, distID):
    df_e = df_race2_all.groupby([distID])['Term Start'].min().reset_index()\
               .rename(columns={'Term Start': 'Earlist Date'})
    df_race2_all = df_race2_all.merge(df_e, left_on=[distID], right_on=[distID], how='outer')
    df_race2_all['Earlist'] = (df_race2_all['Term Start'] == df_race2_all['Earlist Date'])*1.0
    return df_race2_all

def winner_follower(df_race2_all, df_in, distID,rank): #rank='winner', rankID='winnerID'
    rankID = rank+'ID'
    df0 = df_in.groupby([distID, 'Term Start'])['Votes'].max().reset_index() \
        .rename(columns={'Votes': 'Votes Max'})
    df1 = df_race2_all.merge(df0, left_on=[distID, 'Term Start'],right_on=[distID, 'Term Start'], how='outer')
    df1[rank]=(df1['Votes'] == df1['Votes Max']) * 1.0
    df_list = df1.groupby([distID,'Term Start','CandID'])[rank].max().reset_index()
    df_race2_all = df_race2_all.merge(df_list, left_on=[distID,'Term Start','CandID'], right_on=[distID,'Term Start','CandID'], how='outer')
    df_race2_all[rankID] = df_race2_all[rank] * df_race2_all['CandID'].astype(float)
    df_race2_all.loc[df_race2_all[rankID]==0.0,rankID]=None
    return df_race2_all

def key_election(df_race2_all, distID):
    df_k1 = df_race2_all.groupby([distID,'Term Start','RaceID'])['Votes'].sum().reset_index()
    df_k2 = df_k1.groupby([distID, 'Term Start'])['Votes'].max().reset_index().rename(columns={'Votes':'Votes Sum Max'})
    df_k3 = df_k1.merge(df_k2,left_on=[distID,'Term Start'],right_on=[distID,'Term Start'],how='outer')
    df_k3['KeyRace'] = (df_k3['Votes']==df_k3['Votes Sum Max'])*1.0
    df_k3['KeyRaceID'] = (df_k3['Votes']==df_k3['Votes Sum Max'])*df_k3['RaceID'].astype(float)
    df_k3 = df_k3[['KeyRaceID','Votes Sum Max','KeyRace','RaceID']]
    df_race2_all = df_race2_all.merge(df_k3,left_on='RaceID',right_on='RaceID',how='outer')
    return df_race2_all

def win_follow_ever(df_race2_all):
    df = df_race2_all.groupby(['CandID'])['winner','follower','winner_key','follower_key'].max().reset_index()\
         .rename(columns={'winner':'winner ever','follower':'follower ever','winner_key':'winner_key ever','follower_key':'follower_key ever'})
    df_race2_all = df_race2_all.merge(df, left_on='CandID', right_on='CandID', how='outer')
    return df_race2_all

def incumbent_election_v1(df_race2_all, distID):
    df_race2_all['Name Flag'] = (df_race2_all['Name'].str.contains('I')) * 1.0
    df = df_race2_all.groupby([distID, 'Term Start'])['Name Flag'].sum().reset_index()
    df['Incumbent1'] = (df['Name Flag'] > 0.0) * 1.0
    df_race2_all = df_race2_all.merge(df, left_on=[distID, 'Term Start'], right_on=[distID, 'Term Start'], how='outer')
    return df_race2_all

def incumbent_election_v2(df_race2_all, distID):
    df_winner_id = df_race2_all[['winnerID', distID, 'Term Start']]\
                   .groupby([distID, 'Term Start'])['winnerID'].max().reset_index()\
                   .sort_values([distID, 'Term Start'], ascending=True)
    df_winner_id['winnerID previous'] = df_winner_id.groupby([distID])['winnerID'].shift(1)
    df_winner_id = df_winner_id.drop('winnerID', 1)

    df_race2_all = df_race2_all.merge(df_winner_id, left_on=[distID, 'Term Start'],
                                      right_on=[distID, 'Term Start'], how='outer')
    df_race2_all['CandID'] = df_race2_all['CandID'].astype(float)
    df_race2_all['winnerID previous'] = df_race2_all['winnerID previous'].astype(float)
    df_race2_all['Matched'] = (df_race2_all['CandID'] == df_race2_all['winnerID previous']) * 1.0

    df = df_race2_all.groupby([distID,'Term Start'])['Matched'].max().reset_index()
    df = df[df['Matched'] > 0]
    df = df.rename(columns = {'Matched':'Incumbent2'})
    df_race2_all = df_race2_all.merge(df, left_on = [distID,'Term Start'], right_on = [distID, 'Term Start'], how = 'outer')
    df_race2_all.loc[df_race2_all['Incumbent2'] != 1,'Incumbent2'] = (df_race2_all['Earlist'])* 2.0
    # Incumbent2 = {1:incumbent, 0:open, 2:unclear}

    return df_race2_all

def career_span(df_race2_all):
    df1 = df_race2_all.groupby(['CandID'])['Term Start'].agg([min, max]).reset_index().rename(columns={'min':'First Try','max':'Last Try'})
    df_race2_all = df_race2_all.merge(df1, left_on='CandID', right_on='CandID',how='outer')
    return df_race2_all

def first_win(df_race2_all):
    df = df_race2_all.groupby('winnerID')['Term Start'].min().reset_index().rename(columns={'winnerID':'CandID','Term Start':'First Win'})
    df_race2_all = df_race2_all.merge(df, left_on='CandID',right_on='CandID',how='outer')
    return df_race2_all

