# Databricks notebook source
# MAGIC %md
# MAGIC #Product Reference Tables .
# MAGIC
# MAGIC ***This notebook is used to process all product reference tables.***
# MAGIC
# MAGIC - *First select the data file of interest using the widget above***
# MAGIC - *Run each cell below in order of appearance or click on the Run all button***

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "../misc/common_functions"

# COMMAND ----------

# MAGIC %run "../misc/file_path"

# COMMAND ----------

# Widget to select the year of interest
dbutils.widgets.text(name = "period",
                        defaultValue = "2015",
                        label = "Enter Year")

year = dbutils.widgets.get("period")
print(year)

# COMMAND ----------

# Widget to select the data source (ie opioid_ or Stimulant data)
dbutils.widgets.dropdown(
    name = "data_file",
    defaultValue = "lrx_opioid_benzo_nalox_product_ref",
    choices = (
        "lrx_opioid_benzo_nalox_product_ref",
        "lrx_addtnl_mkts_product_ref"
        ),
    label = "Select data file"
    )

data_source = dbutils.widgets.get("data_file")
print(data_source)

# COMMAND ----------

product_ref_df = spark.read.format("delta").load(f"{arei_bronze_path}/{data_source}/{year}")

display(product_ref_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for discrepancies in the total counts and the number of columns between the data and the manifest file.

# COMMAND ----------

actual_columns = product_ref_df.columns
expected_columns = lrx_product_lookup_expected_columns
check_columns(expected_columns, actual_columns)

# COMMAND ----------

# Check counts with manifest
if data_source == "lrx_opioid_benzo_nalox_product_ref":
    manifest_file_path = f"{other_file_path}manifest_files/opioid_benzo_nalox/outputs/{year}"
    manifest_df = spark.read.option("header", "true").csv(manifest_file_path)
else:
    manifest_file_path = f"{other_file_path}manifest_files/addtnl_mkts/outputs/{year}"
    manifest_df = spark.read.option("header", "true").csv(manifest_file_path)

# COMMAND ----------

# manifest_df.display()

# COMMAND ----------

check_row_count(manifest_df, product_ref_df, "product")

# COMMAND ----------

# MAGIC %md
# MAGIC __Checking for number of missing records for each coloumn__

# COMMAND ----------

display(calculate_missing_rows(product_ref_df))

# COMMAND ----------

# MAGIC %md
# MAGIC __Check that both NDC and USC_CD conform to the expected lengths of 11 and 5, respectively, and confirm the absence of any missing leading zeros in the codes__

# COMMAND ----------

column_lengths = dict({
    "NDC_CD": 11,
    "USC_CD": 5,
})

for column, desired_length in column_lengths.items():
    count_invalid_length = product_ref_df.where(length(column) != desired_length).count()
    print(f"Number of rows with invalid length in '{column}': {count_invalid_length} \n")

# COMMAND ----------

product_ref_df.filter(length(col("NDC_CD")) != 11).display()

# COMMAND ----------

product_ref_df.filter(length(col("USC_CD")) != 5).display()

# COMMAND ----------

#Reformat variables to correct length
product_ref_df = (
    product_ref_df
    .withColumn("NDC_CD", 
                when(length("NDC_CD") == 12, substring("NDC_CD", 2, 11))
                .when(length("NDC_CD") < 11 , lpad("NDC_CD", 11, "0"))
                .otherwise(product_ref_df.NDC_CD)
                    )
    )

#check that lengths of new variables are correct  
for column, desired_length in column_lengths.items():
    count = product_ref_df.where(length(column) != desired_length).count()
    print(f"Number of rows with invalid length in the '{column}': {count} \n")

# COMMAND ----------

product_ref_df.filter(length(col("USC_CD")) != 5).dropDuplicates(["NDC_CD"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC __Check the distribution of Administration Route__

# COMMAND ----------

display(
    product_ref_df
    .groupBy("ROUTE_ADM_NM")
    .count()
    .orderBy(col("count").desc())
    )

# COMMAND ----------

#Distribution of the description of the Uniform Dosage Form Code.
display(
    product_ref_df
    .groupBy("DOSAGE_FORM_NM") 
    .count() 
    .orderBy(col("count") 
    .desc())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **Remove duplicates NDC**

# COMMAND ----------

# The current product reference table does not have any duplicates based on NDCs. This because the current data consist of a combine product data IQVIA may have dealt with the duplicates before sharing it with. If this is the case then we may need to understand IQVIA deduplication process. This may not be the case in the future when we receive new datasets where we only receive data for a specific year. As discussed, we need to discuss and work with OI to add an additional data variable to indicate the date the data was received from IQVIA. This will help us to sort the data and ensure that we only keep the most recent record for each patient when deduplicating the data.

# COMMAND ----------

#total number of rows
n = product_ref_df.count()
#Count the total number of distinct NDCs
d = product_ref_df.select("NDC_CD").distinct().count()

print(f"The total number of rows is {n} and the total number of unique NDCs is {d}.")


# COMMAND ----------

#drop duplicates
product_ref_df = product_ref_df.drop_duplicates(["NDC_CD"]) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write delta table to silver folder

# COMMAND ----------

product_ref_df.write.format("delta").mode("overwrite").save(f'{arei_silver_path}/{data_source}/{year}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write csv file to export to Shared Drive

# COMMAND ----------

# update the gold folder and export csvs of the updated files
product_ref_df = (product_ref_df
      .withColumnRenamed("NDC_CD", "NDC")
      .withColumnRenamed("ingestion_date", "INGESTION_DATE")
      )

# COMMAND ----------

# merge_condition = "tgt.NDC_CD = src.NDC_CD" 
# merge_delta_data_other(product_ref_df, data_source, arei_gold_path, merge_condition)

# COMMAND ----------

(
    product_ref_df
    .withColumnRenamed("NDC_CD", "NDC")
    .withColumnRenamed("ingestion_date", "INGESTION_DATE")
    .repartition(1)
    .write
    .option("header",True) 
    .mode("overwrite")
    .csv(f"{csv_exports_path}/LRx/product_reference_table/{data_source}{year}")
)