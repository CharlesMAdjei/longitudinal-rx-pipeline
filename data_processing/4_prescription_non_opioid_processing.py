# Databricks notebook source
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

# MAGIC %md
# MAGIC ## Read in Data

# COMMAND ----------

# Read in additional markets data table for year selected
lrx_df = (
        spark
        .read
        .format("delta")
        .load(f"{arei_silver_path}/lrx_addtnl_mkts/{year}")
        .filter(col("YEAR") == f"{year}")
)

# COMMAND ----------

#Convert all variable names to upper case
lrx_df = lrx_df.select(
    [col(column_name).alias(column_name.upper()) for column_name in lrx_df.columns]
)
lrx_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separate by Market

# COMMAND ----------

market_dict = dict({ "A":"non_opioid_pain",
                     "G":"gabapentin",
                     "M":"muscle_relaxants",
                     "P":"pregabalin",
                     "S":"stimulants",
                     "T":"naltrexone"})
print(market_dict)

# COMMAND ----------

relevant_cols = ["MARKET_NAME", "YEAR", "PATIENT_ID", "CLAIM_ID", "RX_TYPE", "NDC", 
                 "PROVIDER_ZIP", "PROVIDER_STATE", "PROVIDER_ST_CNTY_FIPS_CD",
                 "PROVIDER_SPCL_CD", "DISPENSE_DATE", "MONTH_ID","OPIOID_ER_LA_FLAG",
                 "MARKET_NEW_FLAG","QUANTITY","DAYS_SUPPLY", "PATIENT_PAY",
                 "IQVIA_PRESCRIBER_ID","MODEL_TYP_CD", "NPI",
                 "DAYS_SINCE_LAST_OPIOID", "PATIENT_BRTH_YR", "PATIENT_GENDER", "INGESTION_DATE",
                 "MKTED_PROD_NM", "MKTED_PROD_FORMLTN_NM", "ROUTE_ADM_NM", "DOSAGE_FORM_NM", "UNIFM_PROD_NM", "USC_CD"] #these are columns from the product reference table

lrx_df = lrx_df.select(relevant_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to existing delta tables

# COMMAND ----------

market_names = (
    lrx_df
    .select(col("MARKET_NAME"))
    .distinct()
    .rdd
    .flatMap(lambda x:x)
    .collect()
    )
print(market_names)

# COMMAND ----------

merge_condition = "tgt.CLAIM_ID = src.CLAIM_ID AND tgt.YEAR = src.YEAR" 

for market in market_names:
    market_path = market_dict[market]

    filtered_df = (
        lrx_df
        .where(col('MARKET_NAME') == market)
        .withColumn("UPDATE_DATE", date_format(current_date(),"yyyy-MM-dd"))
        )
    market_path = market_dict[market]
    merge_delta_data(filtered_df, market_path, arei_gold_path, merge_condition, "YEAR")

    #Write csv file to be exported to Shared Drive
    #drop columns association with the product reference table
    (
        filtered_df
        .drop("MKTED_PROD_NM", "MKTED_PROD_FORMLTN_NM", "ROUTE_ADM_NM", "DOSAGE_FORM_NM", "UNIFM_PROD_NM", "USC_CD")
        .repartition(1)
        .write
        .option("header",True) 
        .mode("overwrite")
        .csv(f"{csv_exports_path}/LRx/{market_path}/{year}/")
    )
