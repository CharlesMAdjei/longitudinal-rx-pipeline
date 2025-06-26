# Databricks notebook source
# MAGIC %md
# MAGIC This notebook takes the bronze level data tables, merges them with the patient and product reference tables, performs some data cleaning and reformatting of variables, and writes the final table to the silver folder. This is done by market and year.

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

# Widget to select the data source (ie LRx or Stimulant data)
dbutils.widgets.dropdown(name = "data_file",
                        defaultValue = "lrx_opioid_benzo_nalox",
                        choices = ("lrx_opioid_benzo_nalox",
                                   "lrx_addtnl_mkts"),
                        label = "Select data file")

data_source = dbutils.widgets.get("data_file")
print(data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in Data

# COMMAND ----------

# This PySpark code reads data from a CSV file based on a condition. If the data_source is equal to 'lrx_opioid_benzo_nalox_patient_ref', the code reads the data and filters it to only include rows where the MONTH_ID column starts with the year specified by the year variable. Otherwise, if data_source is not equal to 'lrx_opioid_benzo_nalox_patient_ref', the code filters the data based on the year in the MONTH_ID column and adds a new column called DAYS_SINCE_LAST_OPIOID with a default value of '0'
lrx_df = (
    spark
    .read
    .format("delta")
    .load(f"{arei_bronze_path}/{data_source}/{year}")
    # .where(col("YEAR") == f"{year}")
)

if data_source != 'lrx_opioid_benzo_nalox':
    lrx_df = lrx_df.withColumn("DAYS_SINCE_LAST_OPIOID", lit(None))

display(lrx_df)

# COMMAND ----------

# display( lrx_df.filter(col("MARKET_NAME") == "O").dropDuplicates(subset=["PATIENT_ID"]).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for discrepancies in the columns between the data and the manifest file.

# COMMAND ----------

actual_columns = lrx_df.columns
expected_columns = lrx_expected_columns
check_columns(expected_columns, actual_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patient Counts by Market

# COMMAND ----------

# Total number of records and unique patient IDS
n = lrx_df.count()
pt = lrx_df.select(col("PATIENT_ID")).distinct().count()

print(f"The total number of records is {n} and the number of unique patient IDs is {pt}.")

# COMMAND ----------

# Check counts with manifest
if data_source == "lrx_opioid_benzo_nalox":
    manifest_file_path = f"{other_file_path}manifest_files/opioid_benzo_nalox/outputs/{year}"
    manifest_df = spark.read.option("header", "true").csv(manifest_file_path)
else:
    manifest_file_path = f"{other_file_path}manifest_files/addtnl_mkts/outputs/{year}"
    manifest_df = spark.read.option("header", "true").csv(manifest_file_path)


# Counts form manifest file by MONTH_ID
expected_count = (
    manifest_df
    .filter(col("DATA_TYPE") == "factrx")
    .select(col("MONTH_ID"), col("RECORD_COUNT").alias("EXPECTED_COUNT"))
    .withColumn("EXPECTED_COUNT", col("EXPECTED_COUNT")-1)
    )

# Counts from lrx_df by MONTH_ID
actual_count = lrx_df.groupBy("MONTH_ID").count().orderBy(col("MONTH_ID"))

# Difference
diff_in_count = actual_count.join(expected_count, on="MONTH_ID").withColumn("diff", col("count") - col("EXPECTED_COUNT"))
display(diff_in_count.orderBy(col("MONTH_ID")))

# COMMAND ----------

# Total number of observations by Market name and year
display(
    lrx_df
    .groupBy(["YEAR", "MONTH_ID"])
    .pivot("MARKET_NAME")
    .count() 
    .orderBy(col("MONTH_ID"))
    )
        

# COMMAND ----------

lrx_df.filter(col("MONTH_ID").isNull()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge with Patient Reference Table

# COMMAND ----------

file_path = data_source+"_patient_ref"

pat_ref = (
    spark
    .read
    .format("delta")
    .load(f"{arei_silver_path}/{file_path}/{year}")
    .drop(col("ingestion_date"))
    )

# COMMAND ----------

lrx_pat_df = (
    lrx_df.join(pat_ref, lrx_df.PATIENT_ID ==  pat_ref.PATIENT_ID_REF,"left")
    )
display(lrx_pat_df)

# COMMAND ----------

# Create the merged variable for grouping
lrx_pat_df = (
    lrx_pat_df
    .withColumn("merged_pat_ref", 
                when(col("PATIENT_ID_REF").isNotNull(), 1)
                .otherwise(0)
                )
    )

lrx_pat_df.groupBy(col("merged_pat_ref")).count().display()

# COMMAND ----------

#write a table pf patient IDs that did not merge with the reference table
df_no_patient = (
    lrx_pat_df
    .where(col("merged_pat_ref") == 0)
    .select(col("PATIENT_ID"), col("CLAIM_ID"), col("YEAR"))
    )   
df_no_patient.count()            

# COMMAND ----------

(
    df_no_patient
    .repartition(1)
    .write
    .option("header",True)
    .mode("overwrite")
    .csv(f"{arei_silver_path}/data_quality_check_exports/{data_source}/{year}/patient_notin_LRx")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge with Product Reference Table

# COMMAND ----------

product_ref_file = data_source + "_product_ref"
products_ref = (
    spark
    .read
    .format("delta")
    .load(f"{arei_silver_path}/{product_ref_file}/{year}")
    .drop(col("ingestion_date"))
    )

# COMMAND ----------

#Merge with product reference table
lrx_pro_df = (
    lrx_pat_df.join(products_ref, lrx_pat_df.NDC ==  products_ref.NDC_CD, "left")
    )

# COMMAND ----------

# Create the merged variable to check NDCs that did not merge with product reference table
lrx_pro_df = (
    lrx_pro_df
    .withColumn("merged_prod_ref", when(col("NDC_CD").isNotNull(), 1)
                .otherwise(0))
    )

lrx_pro_df.groupBy(col("merged_prod_ref")).count().display()

# COMMAND ----------

#write a table of NDCs that did not merge with the product reference table and write to silver folder
df_no_NDC = (
    lrx_pro_df
    .where(col("merged_prod_ref") == 0)
    .select(col("PATIENT_ID"), col("CLAIM_ID"), col("YEAR"))
    )   

(
    df_no_NDC
    .repartition(1)
    .write
    .option("header",True)
    .mode("overwrite")
    .csv(f"{arei_silver_path}/data_quality_check_exports/{data_source}/{year}/product_notin_LRx")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reformat Variable Types

# COMMAND ----------

#Rename and cast variables to the appropriate type
lrx_df = (
    lrx_pro_df
    .withColumn("DISPENSE_DATE", to_date(col("DISPENSE_DATE"))) 
    .withColumn("QUANTITY", col("QUANTITY").astype("double")) 
    .withColumn("DAYS_SUPPLY", col("DAYS_SUPPLY").astype("integer")) 
    .withColumn("PATIENT_PAY", col("PATIENT_PAY").astype("double")) 
    .withColumn("DAYS_SINCE_LAST_OPIOID", col("DAYS_SINCE_LAST_OPIOID").astype("integer")) 
    .withColumnRenamed("OPIOID_LA_SA_FLAG", "OPIOID_ER_LA_FLAG")
)
display(lrx_df)    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop VET Specialty

# COMMAND ----------

#Count the number of records with a “VET” value for the PROVIDER_SPCL_CD variable**
vet_count = lrx_df.filter(col("PROVIDER_SPCL_CD") == "VET").count()
print(f"{vet_count} records have VET provider specialties.")

# COMMAND ----------

lrx_df = lrx_df.withColumn(
    "Prov",
    when(col("PROVIDER_SPCL_CD").isNull(), "Missng").otherwise(col("PROVIDER_SPCL_CD"))
)

display(lrx_df.groupBy("Prov").count())

# COMMAND ----------

display(
    lrx_df
    .filter(col("PROVIDER_SPCL_CD")=="VET")
    .groupBy(col("MARKET_NAME"))
    .count()
    .orderBy(col("MARKET_NAME"))
    )

# COMMAND ----------

lrx_df.where(col("PROVIDER_SPCL_CD").isNull()).count()

# COMMAND ----------

#Drop any observation with a “VET” value for the PROVIDER_SPCL_CD variable**, but keep the null values
lrx_novet_df = (
    lrx_df
    .filter((col("PROVIDER_SPCL_CD") != "VET") | (col("PROVIDER_SPCL_CD").isNull()))
)
lrx_novet_df.display()

# COMMAND ----------

lrx_novet_df.where(col("PROVIDER_SPCL_CD").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop non-US states and territories

# COMMAND ----------

#Creating a List of eligible states
states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL",
          "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
          "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
          "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", 
          "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", 
          "WY"]

# COMMAND ----------

# Check for states and territories not found in the list of eligible states
distinct_states = lrx_novet_df.select("PROVIDER_STATE").distinct().rdd.flatMap(lambda x: x).collect()
states_to_excl = list(set(distinct_states) - set(states))
ineligible_row = lrx_novet_df.where(~col("PROVIDER_STATE").isin(states)).count()

print("The data has {} distinct states".format(len(distinct_states)))
print("The states to be excluded are {}".format(states_to_excl))
print("The total number of rows from ineligible states is {}".format(ineligible_row))


# COMMAND ----------

# Count of ineligible states and territories
display(
    lrx_novet_df
    .where(col("PROVIDER_STATE").isin(states_to_excl))
    .groupBy(col("PROVIDER_STATE"))
    .count()
    .orderBy(col("count").desc())
    )

# COMMAND ----------

#Keep records from eligible states only and null values
lrx_states_df = (
    lrx_novet_df
    .where((col("PROVIDER_STATE").isin(states)) | (col("PROVIDER_STATE").isNull()))
    )

# COMMAND ----------

lrx_states_df.where(col("PROVIDER_STATE").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing Data

# COMMAND ----------

display(calculate_missing_rows(lrx_states_df))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Summary Stats for numeric variables

# COMMAND ----------

# MAGIC %md
# MAGIC __QUANTITY__

# COMMAND ----------

display(cal_summary_stats_with_outliers(lrx_states_df, "QUANTITY", "MARKET_NAME"))

# COMMAND ----------

# MAGIC %md
# MAGIC __PATIENT_PAY__

# COMMAND ----------

display(cal_summary_stats_with_outliers(lrx_states_df, "PATIENT_PAY", "MARKET_NAME"))

# COMMAND ----------

lrx_states_df.filter(col("PATIENT_PAY") < 0).count()

# COMMAND ----------

# MAGIC %md
# MAGIC __DAYS_SUPPLY__

# COMMAND ----------

display(cal_summary_stats_with_outliers(lrx_states_df, "DAYS_SUPPLY", "MARKET_NAME"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Counts and Valid values for categorical variables

# COMMAND ----------

#check if NDC has a length of 11 as expected
display(
    lrx_states_df.where(length(col("NDC")) != 11).count()
    )

# COMMAND ----------

#Reformat NDC to correct length
lrx_states_df = (
    lrx_states_df
    .withColumn("NDC", 
                when(length("NDC") == 12, substring("NDC", 2, 11))
                .when(length("NDC") < 11 , lpad("NDC", 11, "0"))
                .otherwise(lrx_states_df.NDC_CD)
                )
    )

# COMMAND ----------

#Check if NPI has a length of 10 as expected
display(
    lrx_states_df
    .where(length(col("NPI")) != 10)
    .count()
    )

# COMMAND ----------

#Check that CHANNEL is only retail
lrx_states_df.select(col("CHNL_CD")).distinct().display()

# COMMAND ----------

# New or refill prescriptionX : Valid responses: N = NRX; R = RRX
lrx_states_df.select(col("RX_TYPE")).distinct().display()

# COMMAND ----------

#Check if Flag for extended-release/long-acting prescription has only these responses L = Long-acting/extended release S = Short-acting
lrx_states_df.select(col("OPIOID_ER_LA_FLAG")).distinct().display()

# COMMAND ----------

#If patient had a prescription filled in the prior 45 days then 'N' otherwise 'Y'
lrx_states_df.select(col("MARKET_NEW_FLAG")).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Write final table to Silver folder

# COMMAND ----------

(
    lrx_states_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f'{arei_silver_path}/{data_source}/{year}')
    )