# Databricks notebook source
# MAGIC %md 
# MAGIC #### run configuration notebook to pass all the common parameters

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/sundarkannan1107@outlook.com/ADB_Team2/configuration_Notebook",100)

# COMMAND ----------

# MAGIC %run ./configuration_Notebook

# COMMAND ----------

# MAGIC %md reading data from customer file

# COMMAND ----------

from pyspark.sql.functions import *

df=spark.read.csv(path="abfss://bayerstorage@bayershackadls.dfs.core.windows.net/raw/customer.csv",inferSchema=True,sep=",",header=True)
df=df.filter(df["phone"].isNotNull())
df=df.dropDuplicates(["customer_id"])
display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("gold.customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.customer

# COMMAND ----------

from pyspark.sql.functions import *

# struct1=StructType([StructField("order_id",IntegerType(),null)])

df_order=spark.read.csv(path="abfss://bayerstorage@bayershackadls.dfs.core.windows.net/raw/order.csv",inferSchema=True,sep=",",header=True)
# df_order=df_order.withColumn("order_year",date_format(col("order_date"), "MM-dd-yyyy"))
# df_order=df_order.withColumn("order_date_n",lit(2))
# df_order=df_order.withColumn("order_date_new",date_format(col("order_date"),'MM-dd-yyyy'))
df_order=df_order.withColumn("order_month",split(col("order_date"),"/")[0])
df_order=df_order.withColumn("order_day",split(col("order_date"),"/")[1])
df_order=df_order.withColumn("order_year",substring(split(col("order_date"),"/")[2],0,4))
# df_order=df_order.withColumn("order_date_new2",date_format(col("order_date_new"),'MM-dd-yyyy'))
display(df_order.limit(5))

# COMMAND ----------

df_order.write.mode("overwrite").format("delta").saveAsTable("gold.order_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.order_new

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from gold.order_new

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### we can get the insights and slice and dice based on country city and year

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select a.customer_id,a.order_year,b.country,b.city,sum(a.total_purchase_value) as total_purchase_value from gold.order_new a inner join gold.customer b on a.customer_id=b.customer_id
# MAGIC where b.active=true
# MAGIC group by a.customer_id,a.order_year,b.country,b.city
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.customer_id,a.order_year,b.country,b.city,sum(a.total_purchase_value) as total_purchase_value from gold.order_new a inner join gold.customer b on a.customer_id=b.customer_id
# MAGIC where b.active=true
# MAGIC group by a.customer_id,a.order_year,b.country,b.city
