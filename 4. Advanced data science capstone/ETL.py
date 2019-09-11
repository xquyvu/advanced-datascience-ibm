from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import lit
from munge import *
import csv
import os

# Create spark context
sc = SparkContext.getOrCreate()
spark = SQLContext.getOrCreate(sc)

# Specify directory
dir_path = './data/extracted/'
train_path = dir_path + 'train.csv'

# Read data
data = read_csv_path(train_path, spark, delimiter=',')

# Get the first 500k rows since we're dealing with 10gb of data
data.createOrReplaceTempView('data')
df = spark.sql('select * from data limit 500000')

# df.show(5)

# df.createOrReplaceTempView('df')
# spark.sql("SELECT max(delivery_perc) as meantemp from df").first()
