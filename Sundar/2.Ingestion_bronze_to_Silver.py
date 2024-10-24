# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import column

# COMMAND ----------



# Initialize Spark session
spark = SparkSession.builder.appName("Data_Quality_check").getOrCreate()

# COMMAND ----------


from pyspark.sql import SparkSession
account_key="kv71aZFlj4Kc73iHhHStyyFc0L9fGdsRq2gj21UxC9qXfvQO7C3+1uAf26zQfQowwJY06zlpNK/Y+AStevNM4A=="
spark.conf.set("fs.azure.account.key.bayershackadls.dfs.core.windows.net",account_key)
#display(dbutils.fs.ls("abfss://bayerstorage@bayershackadls.dfs.core.windows.net/Squad_2/bronze/"))




# COMMAND ----------

#Customer Dataframe 
df_customer = spark.read.format('delta').table('BRONZE.customer')


#Order Dataframe 

df_orders= spark.read.format('delta').table('BRONZE.order')


#Order_line Dataframe 

df_orders_line= spark.read.format('delta').table('BRONZE.orderline')


#customer behavior Dataframe
df_customer_behavior= spark.read.format('delta').table('BRONZE.customerbehavior')



# COMMAND ----------

#remove duplicate

df_customer_cleaned = df_customer.dropDuplicates(["customer_id"])

df_orders_cleaned = df_orders.dropDuplicates(["customer_id"])

df_orders_line_final = df_orders_line.dropDuplicates(["order_id"])

df_customer_behavior_final = df_customer_behavior.dropDuplicates(["customer_id"])



# COMMAND ----------

#if email is Phone number/empty, fill with default value 
df_final_customer = df_customer_cleaned.fillna({"phone": "99999", "email": "Unknown"})

# COMMAND ----------

#if state column is blank ,add Not Available
df_final_orders = df_orders_cleaned.fillna({"state": "Not Available"})

df_final_orders.show(truncate=False)


# COMMAND ----------

#invalid record check 
df_orders_line_final1 = df_orders_line_final.filter(df_orders_line_final.quantity > 0)

# COMMAND ----------

#load customer data
df_final_customer.write.format('delta').mode('overwrite').saveAsTable('SILVER.customer')

#load order data
df_final_orders.write.format('delta').mode('overwrite').saveAsTable('SILVER.order')

#load order_line data
df_orders_line_final1.write.format('delta').mode('overwrite').saveAsTable('SILVER.orderline')

#load customerbehavior data
df_customer_behavior_final.write.format('delta').mode('overwrite').saveAsTable('SILVER.customerbehavior')
