#!/usr/bin/env python
# coding: utf-8

# ## Initialize the Spark environment in Python using the findspark library.

# In[1]:


import findspark
findspark.init('C:\spark\spark-3.4.0-bin-hadoop3-scala2.13')


# # Import the Python and pyspark Library

# In[2]:


import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark # only run this after findspark.init()
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql.types import * 


# # Established a Connection to Spark Cluster

# In[3]:


from pyspark.sql import SparkSession

spark = SparkSession.builder         .appName('Newyork_time Analysis')         .config('spark.executor.instances', '4')         .getOrCreate()


# In[4]:


import time

start_time = time.time()


# In[6]:


# import data
df = spark.read.json('NewYork.json')


# In[7]:


df.show(5)


# In[8]:


df.dtypes


# In[9]:


df.head()


# In[10]:


df.first()


# In[11]:


df.describe().show()


# In[12]:


df.columns


# In[13]:


df.count()


# In[14]:


df.distinct().count()


# # Remove Duplicates values

# In[16]:


df_drop = df.dropDuplicates() 
df_drop.show(5)


# In[17]:


#Show all entries in title column
df.select("author").show(10)


# In[18]:


#Show all entries in title, author, rank, price columns
df.select("author", "title", "rank").show(10)


# In[19]:


df.select("title", when(df.title != 'ODD HOURS', 1).otherwise(0)).show(10)


# In[20]:


df[df.author.isin("Stephenie Meyer", "Emily Giffin")].show(5)


# In[21]:


# Show author and title is TRUE if title has " THE " word in titles
df.select("author", "title", df.title.like("% THE %")).show(15)


# Add columns

# In[25]:


# Update column 'amazon_product_url' with 'URL'
df = df.withColumnRenamed('amazon_product_url', 'URL')

df.show(5)


# # Remove Columns

# In[26]:


df_remove = df.drop("publisher", "published_date").show(5)



# In[27]:


df_remove2 = df.drop(df.publisher).drop(df.published_date).show(5)


# Group By

# In[28]:


df.groupBy("author").count().show(10)


# In[29]:


# Filtering entries of title
# Only keeps records having value 'THE HOST'

df.filter(df["title"] == 'THE HOST').show(5)


# # Handling Missing Value

# In[30]:


# Replace null values using df.na.fill()
df = df.na.fill(0)  # Replace all null values in the DataFrame with 0

# Replace null values using df.fillna()
df = df.fillna(0)  # Replace all null values in the DataFrame with 0



# In[31]:


df.dropna()


# In[32]:


df.na.replace(5, 15)


# In[33]:


# Dataframe with 10 partitions
df.repartition(10).rdd.getNumPartitions()

# Dataframe with 1 partition
df.coalesce(1).rdd.getNumPartitions()


# In[34]:


df.registerTempTable("df")

spark.sql("select * from df").show(3)


# In[35]:


# Converting dataframe into an RDD
rdd_convert = df.rdd


# In[36]:


# Converting dataframe into a RDD of string 
df.toJSON().first()


# In[37]:



# Obtaining contents of df as Pandas 
CSV_data=df.toPandas()


# In[38]:


CSV_data.head()


# In[39]:


CSV_data.describe()


# In[40]:



# Save the DataFrame as a CSV file
CSV_data.to_csv('CSV_data.csv', index=False)


# In[41]:


CSV_data.columns


# In[42]:


CSV_data.isnull().sum()


# In[43]:


# pip install spark-df-profiling


# In[44]:


# pip install pandas-profiling


# In[45]:


import pandas as pd
import pandas_profiling

# Load your data into a DataFrame
df = pd.read_csv("CSV_data.csv")

# Generate the profile report
profile = df.profile_report(title="Pandas Profiling Report")

# Save the report as an HTML file
profile.to_file("profile_report.html")


# In[46]:


CSV_data.info()


# In[47]:




# Count the number of books per author
author_counts = CSV_data['author'].value_counts()

# Get the top authors with the highest number of books
top_authors = author_counts.head(10)  # Change the number as per your preference

# Display the top authors and their book counts
print(top_authors)


# In[48]:


import matplotlib.pyplot as plt

# Get the top authors with the highest number of books
top_authors = author_counts.head(10)  # Change the number as per your preference

# Plotting the top authors
plt.figure(figsize=(10, 6))
top_authors.plot(kind='bar')
plt.xlabel('Author')
plt.ylabel('Number of Books')
plt.title('Top Authors by Number of Books')
plt.xticks(rotation=45)
plt.show()


# In[49]:


# Count the number of books per author
rank = CSV_data['rank'].value_counts()

# Get the top authors with the highest number of books
rank = rank.head(10)  # Change the number as per your preference

# Display the top authors and their book counts
print(rank)


# In[50]:


from collections import Counter


# Step 1: Count the occurrences of each title
title_counts = Counter(CSV_data['title'])

# Step 2: Find the topmost title(s)
top_titles = title_counts.most_common(1)  

# Step 3: Filter books with the topmost title(s)
top_books = CSV_data[CSV_data['title'] == top_titles[0][0]]

# Step 4: Count the occurrences of each author in the top books
author_counts = Counter(top_books['author'])

# Step 5: Find the topmost author(s)
top_authors = author_counts.most_common(1)  

# Print the results
print("Topmost Title:", top_titles[0][0])
print("Topmost Author:", top_authors[0][0])


# In[51]:




# Step 1: Count the occurrences of each title
title_counts = Counter(CSV_data['title'])

# Step 2: Find the topmost title(s)
top_titles = title_counts.most_common(1)  # Change the argument to get more top titles if needed

# Step 3: Filter books with the topmost title(s)
top_books = CSV_data[CSV_data['title'] == top_titles[0][0]]

# Step 4: Count the occurrences of each author in the top books
author_counts = Counter(top_books['author'])

# Step 5: Find the topmost author(s)
top_authors = author_counts.most_common(1)  # Change the argument to get more top authors if needed

# Print the results
print("Topmost Title:", top_titles[0][0])
print("Topmost Author:", top_authors[0][0])

# Step 6: Visualize the author-title relationship
authors = [author for author, _ in author_counts.most_common()]
title_occurrences = [count for _, count in author_counts.most_common()]

plt.bar(authors, title_occurrences)
plt.xlabel('Authors')
plt.ylabel('Number of Books')
plt.title('Author-Title Relationship')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()


# In[52]:




# Step 1: Count the occurrences of each title
title_counts = Counter(CSV_data['title'])

# Step 2: Find the top 10 titles
top_titles = title_counts.most_common(10)

# Step 3: Filter books with the top 10 titles
top_books = CSV_data[CSV_data['title'].isin([title for title, _ in top_titles])]

# Step 4: Count the occurrences of each author in the top books
author_counts = Counter(top_books['author'])

# Step 5: Find the topmost 10 authors
top_authors = author_counts.most_common(10)

# Print the results
print("Top 10 Titles:")
for title, count in top_titles:
    print(title, "-", count)

print("Top 10 Authors:")
for author, count in top_authors:
    print(author, "-", count)


# In[53]:


# Step 6: Visualize the author-title relationship
authors = [author for author, _ in top_authors]
title_occurrences = [count for _, count in top_authors]

plt.bar(authors, title_occurrences)
plt.xlabel('Authors')
plt.ylabel('Number of Books')
plt.title('Author-Title Relationship (Top 10 Books)')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()


# In[54]:


# End the timer
end_time = time.time()

# Print the total time
print("Processing time:", end_time - start_time, "seconds")


# In[ ]:




