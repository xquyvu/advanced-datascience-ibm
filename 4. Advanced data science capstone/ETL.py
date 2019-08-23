from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import lit
from munge import *
import csv
import os

# Create spark context
sc = SparkContext.getOrCreate()
spark = SQLContext.getOrCreate(sc)

# Specify directory
dir_path = './data/extracted/Gas/'

# Check consistent header
check_consistent_header(dir_path)

# Read data

# Explore
# df.count()
# 3085757

# df.show(5)

# df.createOrReplaceTempView('df')
# spark.sql("SELECT max(delivery_perc) as meantemp from df").first()

data = load_data(dir_path, spark, delimiter=',')
data.count()

with open(r'C:\Users\vu86683\Desktop\learn\advanced-datascience-ibm\4. Advanced data science capstone\data\extracted\train.csv') as f:
    reader = csv.reader(f, delimiter=',')
    for i in range(10):
        row = next(reader)
        print(row)