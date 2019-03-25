# Description
This script is comparing two dataframes and produce a short statistic and detailed comparison between them.

**Execute**:

pipenv install

pipenv run python comparison_script.py file1.csv file2.csv

**Example**:

File1

|ID|Col1|Col2|Col3|
|---| ---|---| ---|
|1|1|0|1|
|2|0|1|0|
|3|1|1|1|

File2

|ID|Col1|Col2|Col4|
|---| ---|---| ---|
|1|1|1|1|
|2|1|1|1|
|4|1|1|1|

**Result**:

**columns_statistics_datetime.txt**

Total count of indexes in the first file: 3

Total count of columns in the first file: 3

Columns: ['Col1', 'Col2', 'Col3']

Total count of indexes in the second file: 3

Total count of columns in the second file: 3

Columns: ['Col1', 'Col2', 'Col4']

Count of indexes only in first: 1

Count of indexes only in second: 1
___
**differences_between_first_and_second_prices_datetime.csv**

index,column,in first,in second

1,Col2,0,1

2,Col1,0,1
___
**only_in_first_count_1_datetime.csv**

ID,Col1_x,Col2_x,Col3,Col4

3,1,1,1,
___
**only_in_second_count_1_datetime.csv**

ID,Col1_x,Col2_x,Col4,Col3

4,1,1,1,