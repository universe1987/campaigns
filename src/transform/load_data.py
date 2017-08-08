import re
import json
import pandas as pd
from glob import glob


def keep_ascii(s):
    ascii_part = [c for c in s if ord(c) < 128]
    x = ''.join(ascii_part).strip()
    return ' '.join(x.split())


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


def generate_race_details_table():
    df = select_tables('../../data/json', 'Governor', 'RaceDetails')
    date_cols = ['Polls Close', 'Term Start', 'Term End']
    for column_title in date_cols:
        df[column_title] = pd.to_datetime(df[column_title].str.split('-').str[0], errors='coerce')
    df['State'] = df['Parents'].str.split('>').str[2].str.strip()
    df['Position'] = df['Parents'].str.split('>').str[3].str.strip()
    df['Term Length'] = df['Term End'] - df['Term Start']
    df['ContributorID'] = df['Contributor link'].str.extract('(\d+)', expand=False)
    df['Data Sources'] = df['Data Sources'].str.replace('\[Link\]', "")
    df['Turnout'] = df['Turnout'].str.extract('(\d+\.?\d*)% Total Population', expand=False).astype(float)
    to_drop = ['Contributor link', 'Filing Deadline', 'Filing Deadline link', 'Last Modified', 'Last Modified link',
               'Office link', 'Parents link', 'Polls Close link', 'Polls Open', 'Polls Open link', 'Term End link',
               'Term Start link', 'Turnout link', 'Type link', 'Data Sources link']
    df.drop(to_drop, axis=1, inplace=True)
    return df


def generate_candidate_table():
    df = select_tables('../../data/json', 'Governor', 'Candidates')
    df = df[df['Name'] != '']
    df['CandidateID'] = df['Name link'].str.extract('(\d+)', expand=False)
    df['PartyID'] = df['Party link'].str.extract('(\d+)', expand=False)
    votes_share_df = df['Votes'].str.extract('(?P<votes>\d+,?\d*) \((?P<share>\d+\.?\d*)%\)', expand=False)
    df['Votes'] = votes_share_df['votes']
    df['Share'] = votes_share_df['share']
    to_drop = ["Votes link", "Photo", "Entry Date", "Margin", "Predict Avg.", "Predict Avg. link", "Margin link",
               "Entry Date link", "Cash On Hand", "Cash On Hand link", "Name", "Website link", 'Name link',
               'Party link']
    df.drop(to_drop, axis=1, inplace=True)
    return df


def check_shares_sum(df_race2, df_race):
    # add up the shares in a file which contains votes, shares, per election
    def add_shares(df_race2):
        df_race2['Share'] = df_race2['Share'].astype(float)
        df = df_race2.groupby(['RaceID'])['Share'].sum().reset_index()
        df['Index'] = range(df.shape[0])
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
        df_wrong_shares_full.to_csv("pdata/wrong_shares_full.csv")
        return df_wrong_shares_full

    # return a list of RaceIDs for incorrect races
    def shares_wrong_small(df_wrong_shares_full):
        g = df_wrong_shares_full['RaceID'].unique()
        s = pd.Series(g)
        s.to_csv('pdata/wrong_shares_raceid.csv')

    df_race2_wrong_shares = add_shares(df_race2)
    df_wrong_shares_full = shares_wrong_big(df_race2_wrong_shares, df_race, df_race2)
    shares_wrong_small(df_wrong_shares_full)
    return df_wrong_shares_full


def unique_candidates(df_race2):
    g = df_race2['CandID'].unique()
    df_unique_CandID = pd.DataFrame(pd.Series(g)).rename(columns={0: 'CandID'})
    print 'number of unique candidates=', len(df_unique_CandID)
    df_unique_CandID.to_csv('pdata/unique_CandID.csv')
    return df_unique_CandID


def cand_remove(df, list):
    for x in list:
        df = df[df['CandID'] != x]
    return df


def recent_elections_city(folder):
    path = r'{}'.format(folder)
    df_m = pd.DataFrame()
    for id in [1, 2, 3]:
        df = pd.read_csv(path + "/recent_elections_part{}.txt".format(id), delimiter=';', header=None)
        df_m = df_m.append(df)
    h = df_m.shape[0]
    df_m['CityID'] = range(h)
    dic = {0: 'web', 1: 'city', 2: "state", 3: 'partisan', 4: 'note'}
    for key, value in dic.iteritems():
        df_m = df_m.rename(columns={key: value})
    df_m.to_csv('pdata/recent_elections_city.csv')
    df_m = df_m.drop('note', 1)
    return df_m


def recent_elections_state(folder):
    path = r'{}'.format(folder)
    df_m = pd.DataFrame()
    df_m = pd.read_csv(path + "/governor.csv".format(id), delimiter=',', header=None)
    df_m = df_m[~df_m[0].isnull()]
    h = df_m.shape[0]
    df_m['StateID'] = range(h)
    dic = {0: 'ContainerID', 1: 'State', 2: "year", 3: 'note'}
    for key, value in dic.iteritems():
        df_m = df_m.rename(columns={key: value})
    df_m.to_csv('pdata/recent_elections_state.csv')
    df_m = df_m.drop('note', 1)
    return df_m


def recent_elections(dist, folder_city, folder_state):
    if dist == 'City':
        df_m = recent_elections_city(folder_city)
    else:
        df_m = recent_elections_state(folder_state)
    return df_m


def city_name_merge(df_recent, df_race):
    df_recent['RaceID'] = df_recent['web'].str.extract('(\d+)', expand=False).astype(str)
    df_race['RaceID'] = df_race['RaceID'].astype(str)
    df = df_recent.merge(df_race, left_on='RaceID', right_on='RaceID', how='outer')
    df_city = df[['State', 'County', 'City', 'web', 'state', 'city', 'CityID']]
    df_city.loc[:, 'CityID'] = df_city['CityID'].astype(str)
    df_city = df_city[df_city['CityID'].str.contains('(\d+)')]
    df_city = df_city[df_city['web'].str.contains('http')]
    df_city.to_csv('pdata/city_name_merge.csv')
    return df_city


def state_name_merge(df_recent, df_race):
    df_recent.loc[:, 'State'] = df_recent['State'].str.lower().str.strip()
    df_race.loc[:, 'State'] = df_race['State'].str.lower().str.strip()
    df_state = df_recent.merge(df_race, left_on='State', right_on='State', how='outer')
    df_state = df_state[['State', 'StateID']]
    df_state.to_csv('pdata/state_name_merge.csv')
    return df_state


def dist_name_merge(df_recent, df_race, dist):
    if dist == 'City':
        df_dist = city_name_merge(df_recent, df_race)
    else:
        df_dist = state_name_merge(df_recent, df_race)
    return df_dist


if __name__ == '__main__':
    folder = '../../data/json'
    df = select_tables(folder, 'Governor', 'RaceDetails')
    date_cols = ['Polls Close', 'Term Start', 'Term End']
    keep = ['Contributor', 'Contributor link', 'Data Sources', 'Data Sources link', 'Office', 'Parents', 'Polls Close',
            'Term Start', 'Term End', 'Type', 'Turnout']
    df = csv_to_df('key_race_details.csv', date_cols=date_cols, keep=keep)
    print df.head(5)
