# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "../misc/file_path"

# COMMAND ----------

# Widget to select the data source 
dbutils.widgets.dropdown(
    name = "data_file",
    defaultValue = "opioid_benzo_nalox",
    choices = (
        "opioid_benzo_nalox",
        "addtnl_mkts"
        ),
    label = "Select data file"
    )

data_source = dbutils.widgets.get("data_file")
print(data_source)

# COMMAND ----------

# Widget to select the year of interest
dbutils.widgets.text(name = "period",
                        defaultValue = "2015",
                        label = "Enter Year")

year = dbutils.widgets.get("period")
print(year)

# COMMAND ----------

input_file_path = f"{other_file_path}manifest_files/{data_source}/inputs/{year}"
df = spark.read.option("header", "true").csv(input_file_path)

# COMMAND ----------

df.display()

# COMMAND ----------

df_transformed = (
    df
    .withColumn("FileName", split(col("FileName"), "_")) 
    .withColumn("MARKET", lower(col("FileName").getItem(0))) 
    .withColumn("DATA_TYPE", lower(split(col("FileName").getItem(1), "\.")[0]))
    .withColumn("MONTH_ID", split(col("FileName").getItem(2), "\.")[0]) 
    .withColumnRenamed("RecordCount(With Header Record)", "RECORD_COUNT")
    .withColumn("RECORD_COUNT", regexp_replace(col("RECORD_COUNT"), ",", "").cast("int"))  # Replace commas & Cast to int
    .select(
        "MARKET",
        "DATA_TYPE",
        "MONTH_ID",
        "RECORD_COUNT",
    )
)


# COMMAND ----------

df_transformed.display()

# COMMAND ----------

 output_file_path = f"{other_file_path}manifest_files/{data_source}/outputs/{year}"
 
 (
     df_transformed
     .repartition(1)
     .write
     .option("header",True) 
     .mode("overwrite")
     .csv(f"{output_file_path}")
     )