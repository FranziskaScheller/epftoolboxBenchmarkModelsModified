from epftoolbox.data import read_data

df_train, df_test = read_data(path='.', dataset='DE', begin_test_date='01-01-2016',
                            end_test_date='01-02-2016')
