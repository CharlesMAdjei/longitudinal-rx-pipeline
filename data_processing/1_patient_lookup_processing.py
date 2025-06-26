# Databricks notebook source
# MAGIC %md
# MAGIC # Patient Reference Tables
# MAGIC
# MAGIC
# MAGIC ***This notebook is used to process all patient reference tables.***
# MAGIC
# MAGIC - *First select the data file of interest using the widget above*
# MAGIC - *Run each cell below in order of appearance or click on the Run all button*

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "../misc/common_functions"

# COMMAND ----------

# MAGIC %run "../misc/file_path"

# COMMAND ----------

# Widget to select the data source 
dbutils.widgets.dropdown(
    name = "data_file",
    defaultValue = "lrx_opioid_benzo_nalox_patient_ref",
    choices = (
        "lrx_opioid_benzo_nalox_patient_ref",
        "lrx_addtnl_mkts_patient_ref"
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

# Some columns are renamed conditionally in the select lookup file for consistency. 
# The "PAT_DX_AVAIL_FLAG" column is added to other patient lookup tables exclusively found in the opioid market. 
if data_source == "lrx_opioid_benzo_nalox_patient_ref":
    patient_ref_df = (
        spark
        .read
        .format("delta")
        .load(f"{arei_bronze_path}/{data_source}/{year}")
        .withColumnRenamed("PAT_GENDER_CD", "PATIENT_GENDER") 
        .withColumnRenamed("PAT_BRTH_YR", "PATIENT_BRTH_YR")
        .withColumnRenamed("PATIENT_ID", "PATIENT_ID_REF")
        ) 
else:
    patient_ref_df = (
        spark
        .read
        .format("delta")
        .load(f"{arei_bronze_path}/{data_source}/{year}") 
        .withColumnRenamed("PAT_BRTH_YR", "PATIENT_BRTH_YR") 
        .withColumnRenamed("PATIENT_ID", "PATIENT_ID_REF")
        .withColumn("PAT_DX_AVAIL_FLAG", lit(" ")) #this column is added to that it has the same columns as the opioid_benzo_nalox market
        )
    
# display(patient_ref_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for discrepancies in the total counts and the number of columns between the data and the manifest file.

# COMMAND ----------

actual_columns = patient_ref_df.columns
expected_columns = lrx_patients_lookup_expected_columns
check_columns(expected_columns, actual_columns)

# COMMAND ----------

# Check counts with manifest
if data_source == "lrx_opioid_benzo_nalox_patient_ref":
    manifest_file_path = f"{other_file_path}manifest_files/opioid_benzo_nalox/outputs/{year}"
    manifest_df = spark.read.option("header", "true").csv(manifest_file_path)
else:
    manifest_file_path = f"{other_file_path}manifest_files/addtnl_mkts/outputs/{year}"
    manifest_df = spark.read.option("header", "true").csv(manifest_file_path)


# COMMAND ----------

check_row_count(manifest_df, patient_ref_df, "patient")

# COMMAND ----------

n = patient_ref_df.count()
pt = patient_ref_df.select(col("PATIENT_ID_REF")).distinct().count()

print(f"The number of rows is {n} and the number of unique patient IDs is {pt}.")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

display(calculate_missing_rows(patient_ref_df))

# COMMAND ----------

display(
    patient_ref_df
    .groupBy("PATIENT_BRTH_YR")
    .count()
    .orderBy("PATIENT_BRTH_YR")
    )

# COMMAND ----------

display(
    patient_ref_df.groupBy("PATIENT_GENDER").count()
    )

# COMMAND ----------

patient_ref_df.write.format("delta").mode("overwrite").save(f'{arei_silver_path}/{data_source}/{year}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write csv output to be exported to Shared Drive

# COMMAND ----------

if data_source == "lrx_opioid_benzo_nalox_patient_ref":
    (
        patient_ref_df
        .withColumnRenamed("PATIENT_ID_REF", "PATIENT_ID")
        .withColumnRenamed("ingestion_date", "INGESTION_DATE")
        .repartition(1)
        .write
        .option("header",True) 
        .mode("overwrite")
        .csv(f"{csv_exports_path}/LRx/patient_reference_table/{data_source}/{year}")
    )
else:
    (
        patient_ref_df
        .withColumnRenamed("PATIENT_ID_REF", "PATIENT_ID")
        .withColumnRenamed("ingestion_date", "INGESTION_DATE")
        .drop("PAT_DX_AVAIL_FLAG")
        .repartition(1)
        .write
        .option("header",True) 
        .mode("overwrite")
        .csv(f"{csv_exports_path}/LRx/patient_reference_table/{data_source}/{year}")
    )