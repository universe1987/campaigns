import os
import re
import csv
import json
import time
import collections
import numpy as np
import pandas as pd


def dict2tex2(dic0, col_title, row_title, output, format):
    df = pd.DataFrame({col_title[0]: dic0.keys(), 'temp': dic0.values()}).set_index(col_title[0], drop=False)
    col_format = 'l|l'

    if len(col_title) > 2:
        col_format = 'l|'
        for i in range(len(df['temp'][0])):
            df[col_title[i+1]] = df['temp'].apply(lambda x: x[i]).astype(float)
            col_format = col_format + 'l'
        df = df.drop('temp',1)

    df.rename(columns={'temp':col_title[1]}).reindex(row_title)\
        .to_latex(output, index=False,formatters = format,column_format=col_format)
    return ()

if __name__ == '__main__':
    dic0 = dict()
    dic0['x'] = [1,3]
    dic0['m'] = [4,2]
    dic0['a'] = [4,5]
    col_title = ['name','value1','value2']
    row_title = ['m', 'a', 'x']

    def f1(x):
        return 'blah_%1.2f' % x
    def f2(x):
        return 'f2_%1.2f' % x
    dict2tex2(dic0, col_title, row_title, 'pdata/ppx.tex', [None, f1, f2])
