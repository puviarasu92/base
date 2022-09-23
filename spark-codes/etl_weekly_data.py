#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import col
from pyspark.sql.functions import rand
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import covar_pop
from pyspark.sql.functions import input_file_name
import pandas as pd
import glob
import os
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import DateType
from pyspark.sql.functions import weekofyear

spark = SparkSession.builder.master("local[1]").appName('GCP -Spark ETL').getOrCreate()

# Temptables cleaning process

spark.catalog.dropTempView("trans_info")
spark.catalog.dropTempView("trans_info_s")
spark.catalog.dropTempView("offer_info_s")

# List of table to extract from source server

tablename_list = ['cust_info_s','trans_info_s','offer_info_s']



df_cust_read = spark.read.format("csv").option("header",True).load('gs://cust_info/', Inferschema="True")

df_cust_read.show()

df_cust_read.createOrReplaceTempView("cust_info_s")

df_trans_read = spark.read.format("csv").option("header",True).load('gs://trans_info/', Inferschema="True")

df_trans_read.show()

df_trans_read.createOrReplaceTempView("trans_info_s")

df_offer_read = spark.read.format("csv").option("header",True).load('gs://offer_info/', Inferschema="True")

df_offer_read.show()

df_offer_read.createOrReplaceTempView("offer_info_s")
    
sqlc = SQLContext(spark)

#sqlc.sql("DELETE FROM `gcp-322704:data_analysis_weekly.weekly_cust_data` WHERE true")

df_cust_info = sqlc.sql("select * from cust_info_s")
df_trans_info = sqlc.sql("select * from trans_info_s")
df_offer_info = sqlc.sql("select * from offer_info_s")

#df_cust_info.printSchema()

df_sales_data = df_trans_info.orderBy('cust_id').groupBy('date','cust_id').agg(sum('sales'),count('trans_id')) .withColumnRenamed('sum(sales)', 'total_sales').withColumnRenamed('count(trans_id)', 'visits')

#df_sales_data.show()

df_offer_data_f = df_offer_info.join(df_trans_info,df_offer_info.cust_id ==  df_trans_info.cust_id,'inner') .orderBy(df_offer_info.cust_id).groupBy(df_offer_info.cust_id).agg(sum('offer_redem'),count(df_offer_info.offer_id)) .withColumnRenamed('sum(offer_redem)', 'no_offer_redem').withColumnRenamed('count(offer_id)', 'no_offer_received') 

#df_offer_data_f.show(truncate=False)

#Check trans date is lies in peroid between offer start and end date - if yes offer is redemed and 1, else 0

df_off = df_trans_info.join(df_offer_info,df_trans_info.cust_id ==  df_offer_info.cust_id,'inner') .select(df_trans_info.cust_id,df_trans_info.date,df_offer_info.start_date,df_offer_info.end_date,df_offer_info.offer_id,df_trans_info.offer_redem,
F.when((df_trans_info.date >= df_offer_info.start_date) & (df_trans_info.date <= df_offer_info.end_date), 1) \
.otherwise(0).alias('offer_validity'))

#df_off.show()


df_offer_data_check = df_off.orderBy(df_off.cust_id,) .groupBy(df_off.cust_id,df_off.offer_validity).agg(sum('offer_redem'),count('offer_id')) .withColumnRenamed('sum(offer_redem)', 'no_offer_redem').withColumnRenamed('count(offer_id)', 'no_offer_received') 

#df_offer_data.show(truncate=False)

df_offer_data = df_offer_data_check.withColumnRenamed("cust_id", "off_cust_id")

#df_offer_data.show(truncate=False)

df_trans_data = df_cust_info.join(df_sales_data,df_cust_info.cust_id ==  df_sales_data.cust_id,'left') .join(df_offer_data,df_sales_data.cust_id ==  df_offer_data.off_cust_id,'inner') .select(df_sales_data.date,df_sales_data.cust_id,df_cust_info.cust_name,df_cust_info.cust_dob ,df_sales_data.total_sales,df_offer_data.no_offer_received,df_offer_data.no_offer_redem,df_offer_data.offer_validity,df_sales_data.visits ,round(months_between(current_date(),df_cust_info.cust_dob)/lit(12),2).cast('int').alias('age') ,date_format(to_date(df_sales_data.date),'E').alias('days') ,weekofyear(df_sales_data.date).alias('week_number')) 

#Derive which day the coustomer spends more

final_dfs = df_trans_data.orderBy('cust_id').groupby('cust_id','cust_name','age','week_number','no_offer_received','no_offer_redem','offer_validity','total_sales','visits').pivot('days').max('visits').fillna(0)


#Sampling data - In case of job failure data can be viewed in Spark UI

final_dfs.show()

# Writin the final data into CSV files

#final_dfs.repartition(1).write.mode("overwrite").option("header",False).csv("gs://weekly_sales_data/ics_weekly_data/")

final_dfs.write.json("gs://weekly_sales_data/ics_weekly_data/")


#gs://weekly_sales_data/ics_weekly_data/


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




