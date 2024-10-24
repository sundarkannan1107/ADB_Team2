# Databricks notebook source
def file_check(filename):
    try:
        flag = True
        try:
            file_info = dbutils.fs.ls(filename)
            if len(file_info) == 0:
                raise Exception(f"File {filename} does not exist.")
            
            file_size = file_info[0].size  
            if file_size == 0:
                raise Exception(f"File {filename} exists but is empty.")
            
            print(f"File {filename} exists and is not empty. Size: {file_size} bytes.")
            
        except Exception as e:
            flag = False
            print(f"File check failed for {filename}: {e}")
        return flag
    except Exception as e:
        print(f"Error in file_check: {e}")
        return False


def read_data(filename):
    try:
        if file_check(filename):
            customer_scd2  = spark.read.format('csv').option('header', 'true').option('mode','permissive').option('inferSchema', 'true').load("abfss://bayerstorage@bayershackadls.dfs.core.windows.net/raw/customer_SCD2_data.csv")
          
           # customer_scd2.printSchema()
            customer_scd2.display()
            customer_scd2.write.format('delta').mode('overwrite').saveAsTable('SILVER.customer_SCD2_data')
        else:
            print("One or more files are missing. Cannot read data.")
    except Exception as e:
        print(f"Error in read_data: {e}")

def main():

    account_key="kv71aZFlj4Kc73iHhHStyyFc0L9fGdsRq2gj21UxC9qXfvQO7C3+1uAf26zQfQowwJY06zlpNK/Y+AStevNM4A=="
    spark.conf.set("fs.azure.account.key.bayershackadls.dfs.core.windows.net",account_key)
    filename = 'abfss://bayerstorage@bayershackadls.dfs.core.windows.net/raw/customer_SCD2_data.csv'
    read_data(filename)

if __name__ == "__main__":
    main()




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customer_SCD2_data
