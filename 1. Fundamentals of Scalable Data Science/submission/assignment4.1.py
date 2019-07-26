
# coding: utf-8

# # Assignment 4
# 
# Welcome to Assignment 4. This will be the most fun. Now we will prepare data for plotting.
# 
# Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook. Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# Sampling is one of the most important things when it comes to visualization because often the data set gets so huge that you simply
# 
# - can't copy all data to a local Spark driver (Watson Studio is using a "local" Spark driver)
# - can't throw all data at the plotting library
# 
# Please implement a function which returns a 10% sample of a given data frame:

# In[1]:


def getSample(df,spark):
    return df.sample(fraction=.1)


# Now we want to create a histogram and boxplot. Please ignore the sampling for now and return a python list containing all temperature values from the data set

# In[2]:


def getListForHistogramAndBoxPlot(df,spark):
    query = 'SELECT temperature from washing where temperature is not null'
    my_list = spark.sql(query).rdd.map(lambda row: row.temperature).collect()
    
    if not type(my_list)==list:
        raise Exception('return type not a list')

    return my_list


# Finally we want to create a run chart. Please return two lists (encapsulated in a python tuple object) containing temperature and timestamp (ts) ordered by timestamp. Please refer to the following link to learn more about tuples in python: https://www.tutorialspoint.com/python/python_tuples.htm

# In[3]:


def getListsForRunChart(df,spark):
    query = 'select ts, temperature from washing where temperature is not null order by ts asc'
    double_tuple_rdd = spark.sql(query).sample(False,0.1).rdd.map(lambda row : (row.ts, row.temperature))
    
    result_array_ts = double_tuple_rdd.map(lambda ts_temperature: ts_temperature[0]).collect()
    result_array_temperature = double_tuple_rdd.map(lambda ts_temperature: ts_temperature[1]).collect()
    
    return (result_array_ts,result_array_temperature)


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to grab a PARQUET file and create a dataframe out of it. Using SparkSQL you can handle it like a database. 

# In[4]:


get_ipython().system(u'wget https://github.com/IBM/coursera/blob/master/coursera_ds/washing.parquet?raw=true')
get_ipython().system(u'mv washing.parquet?raw=true washing.parquet')


# In[ ]:


df = spark.read.parquet('washing.parquet')
df.createOrReplaceTempView('washing')
df.show()


# In[ ]:


get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt


# In[ ]:


plt.hist(getListForHistogramAndBoxPlot(df,spark))
plt.show()


# In[ ]:


plt.boxplot(getListForHistogramAndBoxPlot(df,spark))
plt.show()


# In[ ]:


lists = getListsForRunChart(df,spark)


# In[ ]:


plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()


# Congratulations, you are done! Please download the notebook as python file, name it assignment4.1.py and submit it to the grader.
