def check_share_sum(df):
    df_sum = df.groupby(['RaceID'])['Share'].sum().reset_index()
    bad_rows = df_sum[abs(df_sum['Share'] - 100) >= .1]
    if len(bad_rows) == 0:
        print 'no bad rows'
    else:
        print 'bad rows:'
        print bad_rows


def fix_bad_share(df):
    epsilon = 1e-10
    df['Share'] += epsilon
    df_sum = df.groupby(['RaceID'])['Share'].sum().reset_index()
    df_sum.rename(columns={'Share': 'Total'}, inplace=True)
    df_m = df.merge(df_sum, left_on='RaceID', right_on='RaceID')
    df_m['Share'] = 100 * df_m['Share'] / df_m['Total']
    df_m.drop(['Total'], axis=1, inplace=True)
    return df_m


def keep_ascii(s):
    ascii_part = [c for c in s if ord(c) < 128]
    x = ''.join(ascii_part).strip()
    return ' '.join(x.split())
