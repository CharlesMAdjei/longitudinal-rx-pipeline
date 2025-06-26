# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#Create widgets for selecting Market Name and Year to write .csv
dbutils.widgets.dropdown(name = "year",
                        defaultValue = "2024",
                        choices = ("2015", "2016", "2017", "2018", "2019","2020","2021","2022","2023", "2024"),
                        label = "Select Year")
year = dbutils.widgets.get("year")

print(year)

# COMMAND ----------

container = "abfss://ddnid-ncipc@edavsynapsedatalake.dfs.core.windows.net"
source_path = f"{container}/DOP/IQVIA/gold/LRx"
sink_path = f"{container}/DOP/IQVIA/gold/csv_exports/LRx"

# COMMAND ----------

markets = ["opioid", "naloxone", "buprenorphine", "benzodiazepines", "other_sedatives","gabapentin","stimulants","pregabalin", "muscle_relaxants", "naltrexone","non_opioid_pain"]

# COMMAND ----------

# MAGIC %md
# MAGIC #Data files

# COMMAND ----------

for market in markets:
    spark.read.format("delta").load(f"{source_path}/{market}").where(col("YEAR") == f"{year}") \
        .repartition(1).write.option("header",True).mode("overwrite").csv(f"{sink_path}/{market}/{year}")   