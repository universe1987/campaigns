import pandas as pd

#=============================================================================#
#                   The Purpose
#
#  Import One Dataframe, Row and Column Names, Export tex Table
#
#  Import Two Dataframes, Row and Column Names, Export a joint tex Table
#
#=============================================================================#

#=============================================================================#
#                   Warning
#
# Cannot handle string tables
# "%8.2f" Use 8 characters, give 2 decimal places
#=============================================================================#

def df2tex(file1, folder, file2, formats, integer_index, cols, row_name, col_name):
    if integer_index == 1:
        df = file1.iloc[:,cols]
    else:
        df = file1.loc[:,cols]
    h,w = df.shape

    if formats is None:
        for i in range(h):
            for j in range(w):
                df.iloc[i,j] = str(df.iloc[i,j])
    else:
        for i in range(h):
            for j in range(w):
                df.iloc[i,j] = formats % df.iloc[i,j]

    # Import Row Names
    if integer_index == 1:
        rown = file1.iloc[:,row_name]
    else:
        rown = file1.loc[:,row_name]
    rowc = len(row_name)
    w3 = 1 + w

    # Export File
    with open(folder + file2, 'w') as f2:
        if col_name is None:
            f2.write(r'\begin{{tabular}}{{*{{{}}}{{l}}*{{{}}}{{c}}}}'.format(rowc, w) +'\n'  )
        else:
            f2.write(r'\begin{{tabular}}{{*{{{}}}{{l|}}*{{{}}}{{c}}}}'.format(rowc, w) + '\n')

        if row_name is not None:
            f2.write(r'\hline\hline' + '\n')
        else:
            f2.write(r'\cline{2-' + str(w3) + r'}' + r'\cline{2-' + str(w3) + r'}' + '\n')

        if col_name is not None:
            for i in range(rowc-1):
                f2.write('&')
            for j in range(w):
                f2.write('&' + str(col_name[j]))

        if row_name is not None:
            f2.write(r'\\\hline' + '\n')
        else:
            f2.write(r'\\\cline{2-' + str(w3) + r'}' + '\n')

        for i in range(h):
            for k in range(len(row_name)):
                f2.write(str(rown.iloc[i,k]))
            for j in range(w):
                f2.write('&'+str(df.iloc[i,j]))
            f2.write(r'\\'+'\n')

        if row_name is not None:
            f2.write(r'\hline\hline' + '\n')
        else:
            f2.write(r'\cline{2-' + str(w3) + r'}' + '\n')

        f2.write(r'\end{tabular}')
    f2.close()

def df2tex2(file1, file3, folder, file2, formats, integer_index, cols, row_name, col_name, left, right):
    # Import First File
    if integer_index == 1:
        df1 = file1.iloc[:,cols]
    else:
        df1 = file1.loc[:,cols]
    h,w = df1.shape

    if formats is None:
        for i in range(h):
            for j in range(w):
                df1.iloc[i,j] = str(df1.iloc[i,j])
    else:
        for i in range(h):
            for j in range(w):
                df1.iloc[i,j] = formats % df1.iloc[i,j]

    # Import Second File
    if integer_index == 1:
        df3 = file3.iloc[:, cols]
    else:
        df3 = file3.loc[:, cols]
    h, w = df3.shape

    if formats is None:
        for i in range(h):
            for j in range(w):
                df3.iloc[i, j] = str(df3.iloc[i, j])
    else:
        for i in range(h):
            for j in range(w):
                df3.iloc[i, j] = formats % df3.iloc[i, j]

    # Import Row Names
    if integer_index == 1:
        rown = file1.iloc[:,row_name]
    else:
        rown = file1.loc[:,row_name]
    rowc = len(row_name)

    w3 = 1 + 2 * w

    # Export File
    with open(folder + file2, 'w') as f2:
        w2 = 2 * w
        if col_name is not None:
            f2.write(r'\begin{{tabular}}{{|*{{{}}}{{l|}}*{{{}}}{{c|}}}}'.format(rowc, w2) + '\n')
        else:
            f2.write(r'\begin{{tabular}}{{*{{{}}}{{l}}|*{{{}}}{{c|}}}}'.format(rowc, w2) + '\n')

        if row_name is not None:
            f2.write(r'\hline\hline' + '\n')
        else:
            f2.write(r'\cline{2-' + str(w3) + r'}' + r'\cline{2-' + str(w3) + r'}' + '\n')

        if col_name is not None:
            for i in range(rowc - 1):
                f2.write('&')
            for j in range(w):
                f2.write('&' + r'\multicolumn{2}{|c|}{' + str(col_name[j]) + r'}')

        if row_name is not None:
            f2.write(r'\\\hline' + '\n')
        else:
            f2.write(r'\\\cline{2-' + str(w3) + r'}' + '\n')

        for i in range(rowc - 1):
            f2.write('&')
        for j in range(w):
            f2.write('&' + str(left) + '&' + str(right))

        if row_name is not None:
            f2.write(r'\\\hline' + '\n')
        else:
            f2.write(r'\\\cline{2-' + str(w3) + r'}' + '\n')

        for i in range(h):
            for k in range(len(row_name)):
                f2.write(str(rown.iloc[i,k]))
            for j in range(w):
                f2.write('&' + str(df1.iloc[i,j]) + '&' + str(df3.iloc[i,j]))
            f2.write(r'\\' + '\n')

        if row_name is not None:
            f2.write(r'\hline' + '\n')
        else:
            f2.write(r'\cline{2-' + str(w3) + r'}' + '\n')

        f2.write(r'\end{tabular}')
    f2.close()


if __name__ == '__main__':
    dic0 = dict()
    dic0['x'] = 1
    dic0['m'] = 2
    dic0['a'] = 4
    df_test = pd.DataFrame({'variable': dic0.keys(), 'value': dic0.values()}).set_index('variable', drop=False)
    print df_test
    print '----------------------'

    df2tex(df_test, '/Users/yuwang/Dropbox/local politicians/model/analysis_state/','file2.tex', "%8.2f", 0, ['value'], ['variable'], ['value'])
    df2tex(df_test, '/Users/yuwang/Dropbox/local politicians/model/analysis_state/','file2.tex', "%8.2f", 1, [0], [1], ['value'])
    df2tex2(df_test, df_test,'/Users/yuwang/Dropbox/local politicians/model/analysis_state/', 'file2.tex', "%8.2f", 0, ['value'],
       ['variable'], ['value'],'left','right')
