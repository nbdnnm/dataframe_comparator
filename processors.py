from datetime import datetime
import pandas as pd
import numpy as np


timestamp = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')


def save_column_statistics(column_statistics, dataframe, file_name):
    with open(column_statistics, 'a+') as column_statistics_file:
        column_statistics_file.write(
            'Total count of indexes in the ' + file_name + ': ' + str(len(dataframe.index))
            + '\nTotal count of columns in the ' + file_name + ': ' + str(len(dataframe.columns))
            + '\nColumns: ' + str(dataframe.columns.tolist()) + '\n')


def columns_which_are_only_in_one_file(first_dataframe, second_dataframe):
    indexes_only_in_first = get_entries_which_are_only_in_one_file(first_dataframe, second_dataframe)
    indexes_only_in_second = get_entries_which_are_only_in_one_file(second_dataframe, first_dataframe)

    indexes_only_in_first = remove_empty_columns_after_join(indexes_only_in_first)
    indexes_only_in_second = remove_empty_columns_after_join(indexes_only_in_second)

    return indexes_only_in_first, indexes_only_in_second


def save_columns_differences_statistics(file_name, indexes_only_in_first, indexes_only_in_second):
    indexes_only_in_first_count = str(len(indexes_only_in_first.index.tolist()))
    indexes_only_in_first.to_csv('only_in_first_count_' + indexes_only_in_first_count + '_' + timestamp + '.csv')

    indexes_only_in_second_count = str(len(indexes_only_in_second.index.tolist()))
    indexes_only_in_second.to_csv('only_in_second_count_' + indexes_only_in_second_count + '_' + timestamp + '.csv')

    with open(file_name, 'a+') as columns:
        columns.write('\nCount of indexes only in first: ' + indexes_only_in_first_count)
        columns.write('\nCount of indexes only in second: ' + indexes_only_in_second_count)


def remove_empty_columns_after_join(indexes_only_in_first):
    return indexes_only_in_first[
        indexes_only_in_first.columns.drop(list(indexes_only_in_first.filter(regex='.*_y')))]


def get_entries_which_are_only_in_one_file(first_dataframe, second_dataframe):
    return (first_dataframe.merge(second_dataframe, left_index=True, right_index=True, how='left', indicator=True).query(
        '_merge == "left_only"').drop('_merge', 1))


def find_differences_in_values_per_indexes_and_column(first_dataframe, second_dataframe):
    first_dataframe_for_comparison, second_dataframe_for_comparison = get_tables_with_only_overlapping_indexes_and_columns(
        first_dataframe,
        second_dataframe)

    columns_comparison = compare_dataframes(first_dataframe_for_comparison, second_dataframe_for_comparison)

    return columns_comparison


def save_detailed_differences_comparison(columns_comparison):
    columns_comparison[
        ~((columns_comparison['in first'] == '') & (columns_comparison['in second'] == ''))].to_csv(
        'differences_between_first_and_second_prices_' + timestamp + '.csv')


def compare_dataframes(first_file_columns_for_comparison, second_file_columns_for_comparison):
    diff_stack = (first_file_columns_for_comparison != second_file_columns_for_comparison).stack()
    changed = diff_stack[diff_stack]
    changed.index.names = ['index', 'column']
    cells_with_differences = np.where(
        first_file_columns_for_comparison != second_file_columns_for_comparison)

    values_in_first_file = first_file_columns_for_comparison.values[cells_with_differences]
    values_in_second_file = second_file_columns_for_comparison.values[cells_with_differences]
    columns_comparison = pd.DataFrame({'in first': values_in_first_file, 'in second': values_in_second_file},
                                         index=changed.index)
    return columns_comparison


def get_tables_with_only_overlapping_indexes_and_columns(first_dataframe, second_dataframe):
    indexess_in_both_files = set(first_dataframe.index.tolist()).intersection(
        second_dataframe.index.tolist())
    columns_in_both_files = list(
        set(first_dataframe.columns.tolist()).intersection(second_dataframe.columns.tolist()))

    first_file_columns_for_comparison = get_table_only_with_indexes(indexess_in_both_files, first_dataframe)
    second_file_columns_for_comparison = get_table_only_with_indexes(indexess_in_both_files, second_dataframe)

    first_file_columns_for_comparison = get_table_only_with_columns(columns_in_both_files,
                                                                       first_file_columns_for_comparison)
    second_file_columns_for_comparison = get_table_only_with_columns(columns_in_both_files,
                                                                        second_file_columns_for_comparison)

    return first_file_columns_for_comparison, second_file_columns_for_comparison


def get_table_only_with_columns(columns_in_both_files, first_file_columns_for_comparison):
    return first_file_columns_for_comparison[columns_in_both_files]


def get_table_only_with_indexes(indexess_in_both_files, first_dataframe):
    return first_dataframe.loc[indexess_in_both_files]
