import sys
from datetime import datetime
import pandas as pd
from processors import *

first_file_dataframe = pd.read_csv(sys.argv[1], sep=',', header=0, index_col=0, dtype=str)
second_file_dataframe = pd.read_csv(sys.argv[2], sep=',', header=0, index_col=0, dtype=str)

first_file_dataframe = first_file_dataframe.replace(np.nan, '', regex=True)
second_file_dataframe = second_file_dataframe.replace(np.nan, '', regex=True)
first_file_dataframe.index = first_file_dataframe.index.map(str)
second_file_dataframe.index = second_file_dataframe.index.map(str)

timestamp = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')

columns_statistics_filename = 'columns_statistics_' + timestamp + '.txt'

print('Save columns statistics')
save_column_statistics(columns_statistics_filename, first_file_dataframe, 'first file')
save_column_statistics(columns_statistics_filename, second_file_dataframe, 'second file')

print('Save columns which are only in one file')
indexes_only_in_first, indexes_only_in_second = columns_which_are_only_in_one_file(first_file_dataframe,
                                                                                   second_file_dataframe)
save_columns_differences_statistics(columns_statistics_filename, indexes_only_in_first, indexes_only_in_second)

print('Save detailed differences per index per column')
columns_comparison = find_differences_in_values_per_indexes_and_column(first_file_dataframe,
                                                                       second_file_dataframe)
save_detailed_differences_comparison(columns_comparison)
print('Done')
