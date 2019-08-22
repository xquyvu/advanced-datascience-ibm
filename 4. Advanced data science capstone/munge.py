import os
import csv
from pyspark.sql.functions import lit
from functools import reduce


def get_year(path):
    return int(path[-8:-4])


def read_csv_path(path, spark, delimiter=','):
    frame = spark.read\
        .format("csv")\
        .option("header", "true")\
        .option('delimiter', delimiter)\
        .load(path)
    return frame


def check_consistent_header(dir_path):
    header_dict = {}

    for file_name in os.listdir(dir_path):
        with open(dir_path + file_name) as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            header_dict[file_name] = next(reader)

    # Check if column names are the same
    for file_name in header_dict.keys():
        ref = set(header_dict[list(header_dict.keys())[0]])

        if set(header_dict[file_name]) == ref:
            pass
        else:
            print('mismatch')
            return header_dict

        print('header consistent')
        return True


def merge_all_dfs(df_list):
    return reduce(lambda x, y: x.union(y), df_list)


def load_data(dir_path, spark, delimiter=','):
    # Initialise list to store data
    df_list = []

    for file_name in os.listdir(dir_path):
        # Get year
        year = get_year(file_name)

        # Read data
        frame = read_csv_path(dir_path + file_name, spark, delimiter)

        # Add year column to the data
        frame = frame.withColumn('year', lit(year))

        # Store
        df_list.append(frame)

    # Combine
    df = merge_all_dfs(df_list)

    return df
