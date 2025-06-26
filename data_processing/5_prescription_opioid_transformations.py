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

#other files needed. Make sure these are updated to latest version name
benzo_market_def = "CDC Q4 2024 Benzo New Product Workbook.xlsx"
opioid_market_def = "CDC Q4 2024 Opioid New Product Workbook.xlsx"
mme_file = "opioids_2023.sas7bdat"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in Data

# COMMAND ----------

lrx_df = (
    spark
    .read
    .format("delta")
    .load(f"{arei_silver_path}/lrx_opioid_benzo_nalox/{year}")
    )

# COMMAND ----------

#Convert all variable names to upper case
lrx_df = lrx_df.select(
    [col(column_name).alias(column_name.upper()) for column_name in lrx_df.columns]
)

display(lrx_df.limit(5))

# COMMAND ----------

#Select final columns to include in gold tables
final_cols = ["MARKET_NAME", "YEAR", "PATIENT_ID", "CLAIM_ID", "RX_TYPE", "NDC", 
                 "PROVIDER_ZIP", "PROVIDER_STATE", "PROVIDER_ST_CNTY_FIPS_CD",
                 "PROVIDER_SPCL_CD", "DISPENSE_DATE", "MONTH_ID","OPIOID_ER_LA_FLAG",
                 "MARKET_NEW_FLAG","QUANTITY","DAYS_SUPPLY", "PATIENT_PAY",
                 "IQVIA_PRESCRIBER_ID","MODEL_TYP_CD", "NPI",
                 "DAYS_SINCE_LAST_OPIOID", "PATIENT_BRTH_YR", "PATIENT_GENDER", "PAT_DX_AVAIL_FLAG", "INGESTION_DATE",
                 "MKTED_PROD_NM", "MKTED_PROD_FORMLTN_NM", "ROUTE_ADM_NM", "DOSAGE_FORM_NM", "UNIFM_PROD_NM", "USC_CD"] #these are columns from the product reference table

lrx_df = lrx_df.select(final_cols)
lrx_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Naloxone

# COMMAND ----------

merge_condition = "tgt.CLAIM_ID = src.CLAIM_ID AND tgt.YEAR = src.YEAR" 

filtered_df = (
    lrx_df
    .where(col('MARKET_NAME') == 'N')
    .withColumn("UPDATE_DATE", date_format(current_date(),"yyyy-MM-dd"))
)

market_path = "naloxone"
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

# COMMAND ----------

naloxone = spark.read.format("delta").load(f"{arei_gold_path}/naloxone")
display(naloxone.groupBy("YEAR").count().orderBy("YEAR"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benzos and Other Sedatives

# COMMAND ----------

# MAGIC %md
# MAGIC ### Market Defintion

# COMMAND ----------

benzo_mkt_def_file = (
    spark
    .read
    .format("com.crealytics.spark.excel")
    .option("header",True)
    .load(other_file_path + benzo_market_def)
    )

display(benzo_mkt_def_file.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Benzo Flag

# COMMAND ----------

#Create benzo flad based on "PROD_CUSTOM_LVL1_DESC"
benzo_flag = (
    benzo_mkt_def_file
    .withColumn("BENZO_FLAG", when(col("PROD_CUSTOM_LVL1_DESC")=="BENZODIAZEPINES", "B")
                            .when(col("PROD_CUSTOM_LVL1_DESC")=="OTHER", "OS")
                            .otherwise(None))
    )

benzo_flag.groupBy("BENZO_FLAG").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge Benzo Flag and create B and OS markets

# COMMAND ----------

benzo_os_df = (
    lrx_df
    .where(col('MARKET_NAME') == 'B')
    .drop('MARKET_NAME')
)
display(benzo_os_df.limit(10))

# COMMAND ----------

#Merge benzo and other sedatives with market defintion containing "BENZO_FLAG" variable and check that all rows in data merged with this flag
benzo_flag_df = (
    benzo_os_df.join(benzo_flag.select(["NDC", "BENZO_FLAG"]), "NDC", "left")
    .withColumnRenamed("BENZO_FLAG", 'MARKET_NAME')
    .withColumn("merged", when(col('MARKET_NAME').isNotNull(), 1).otherwise(0))
    )

benzo_flag_df.groupBy("merged").count().display()
benzo_flag_df.groupBy('MARKET_NAME').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Delta Tables to Gold and CSVs

# COMMAND ----------

market_dict = dict({ "B":"benzodiazepines", "OS":"other_sedatives"})
market_names = benzo_flag_df.select(col("MARKET_NAME")).distinct().rdd.flatMap(lambda x:x).collect()
print(market_names)

# COMMAND ----------

merge_condition = "tgt.CLAIM_ID = src.CLAIM_ID AND tgt.YEAR = src.YEAR" 

for market in market_names:
    filtered_df = (
        benzo_flag_df
        .where(col('MARKET_NAME') == market)
        .select(final_cols)
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

# COMMAND ----------

benzo = spark.read.format("delta").load(f"{arei_gold_path}/benzodiazepines")
display(benzo.groupBy("YEAR").count().orderBy("YEAR"))

# COMMAND ----------

other = spark.read.format("delta").load(f"{arei_gold_path}/other_sedatives")
display(other.groupBy("YEAR").count().orderBy("YEAR"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opioids and Buprenorphine

# COMMAND ----------

# MAGIC %md
# MAGIC ### Market Definition and MME File

# COMMAND ----------

opioid_mkt_def_file = (
    spark
    .read
    .format("com.crealytics.spark.excel")
    .option("header",True)
    .load(other_file_path + opioid_market_def)
)
opioid_mkt_def_file.display()

# COMMAND ----------

#Add column "MARKET_NAME" identifying opioid and buprenorphine naloxone markets based on USC_CD
    #Buprenorphine Market: USC_CD = 78340 is "DRUG DEPENDENCE", but exclude Methadone HCl
    #Opioid Markeyt: everything else
#Create MOUD flad based on USC_CD = 78340 (Drug Dependence)

opioid_mkt_def_file = (
    opioid_mkt_def_file
    .withColumn("MARKET_NAME", when((col("USC_CD") == "78340") & (col("GPI_DESC") != "Methadone HCl Conc 10 MG/ML"), "BU")
                            .otherwise("O")
               )
    .withColumn("MOUD", when(col("USC_CD") == "78340", 1)
                .otherwise(0)
                )
)

opioid_mkt_def_file.groupBy(["MARKET_NAME", "MOUD"]).count().display()

# COMMAND ----------

opioids_mme_df = (
    spark
    .read
    .format("com.github.saurfang.sas.spark")
    .load(other_file_path + mme_file)
    )
display(opioids_mme_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge with MME file

# COMMAND ----------

opioid_bupe_df = (
    lrx_df
    .where(col('MARKET_NAME') == 'O')
    .drop('MARKET_NAME')
    .join(opioids_mme_df, "NDC", "left")
    .drop("NDC_Numeric")
    .withColumn("merged", when(col("PRODNME").isNotNull(), 1).otherwise(0))
)

#Convert all variable names to upper case
opioid_bupe_df = opioid_bupe_df.select(
    [col(column_name).alias(column_name.upper()) for column_name in opioid_bupe_df.columns]
)

opioid_bupe_df.groupBy("MERGED").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmatched NDCs

# COMMAND ----------

unmatched_ndc = (
    opioid_bupe_df
    .filter(col("MERGED") == 0)
    .groupBy("NDC")
    .count()
    .orderBy(col("count").desc())
)

display(unmatched_ndc)

unmatched_ndc.repartition(1).write.option("header",True).mode("overwrite").csv(f"{arei_silver_path}/data_quality_check_exports/lrx_opioid_benzo_nalox/{year}/unmatched_opioid_ndc")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Buprenorphine Market

# COMMAND ----------

bupe_cols = ["MARKET_NAME", "YEAR", "PATIENT_ID", "CLAIM_ID", "RX_TYPE", "NDC",
                 "PROVIDER_ZIP", "PROVIDER_STATE", "PROVIDER_ST_CNTY_FIPS_CD",
                 "PROVIDER_SPCL_CD", "DISPENSE_DATE", "MONTH_ID","OPIOID_ER_LA_FLAG",
                 "MARKET_NEW_FLAG","QUANTITY","DAYS_SUPPLY", "PATIENT_PAY",
                 "IQVIA_PRESCRIBER_ID","MODEL_TYP_CD", "NPI", "DAYS_SINCE_LAST_OPIOID",
                 "PATIENT_BRTH_YR", "PATIENT_GENDER", "PAT_DX_AVAIL_FLAG", 
                 "MKTED_PROD_NM", "MKTED_PROD_FORMLTN_NM", "ROUTE_ADM_NM", "DOSAGE_FORM_NM", "UNIFM_PROD_NM", "USC_CD",
                 "PRODNME", "GENNME", "MASTER_FORM", "CLASS", "DRUG", "LONGSHORTACTING",
                 "DEACLASSCODE", "STRENGTH_PER_UNIT", "UOM", "MME_CONVERSION_FACTOR",
                 "MME_CONVERSION_FACTOR_N", "INGESTION_DATE"]

# COMMAND ----------

df_bupe = (
    opioid_bupe_df
    .join(opioid_mkt_def_file.select(["NDC", "MARKET_NAME"]), "NDC", "left")
    .where(col('MARKET_NAME') == "BU")
    .select(bupe_cols)
)
display(df_bupe.limit(20))

# COMMAND ----------

#write delta table to gold folder
df_bupe = (
    df_bupe
    .withColumn("UPDATE_DATE", date_format(current_date(),"yyyy-MM-dd"))
)

merge_condition = "tgt.CLAIM_ID = src.CLAIM_ID AND tgt.YEAR = src.YEAR" 
market_path = "buprenorphine"
merge_delta_data(df_bupe, market_path, arei_gold_path, merge_condition, "YEAR")

#Write csv file to be exported to Shared Drive
#drop columns association with the product reference table
(
    df_bupe
    .drop("MKTED_PROD_NM", "MKTED_PROD_FORMLTN_NM", "ROUTE_ADM_NM", "DOSAGE_FORM_NM", "UNIFM_PROD_NM", "USC_CD")
    .repartition(1)
    .write
    .option("header",True) 
    .mode("overwrite")
    .csv(f"{csv_exports_path}/LRx/{market_path}/{year}/")
)

# COMMAND ----------

bupe = spark.read.format("delta").load(f"{arei_gold_path}/buprenorphine")
display(bupe.groupBy("YEAR").count().orderBy("YEAR"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opioid Market

# COMMAND ----------

opioid_cols = ["MARKET_NAME", "YEAR", "PATIENT_ID", "CLAIM_ID", "RX_TYPE", "NDC",
                 "PROVIDER_ZIP", "PROVIDER_STATE", "PROVIDER_ST_CNTY_FIPS_CD",
                 "PROVIDER_SPCL_CD", "DISPENSE_DATE", "MONTH_ID","OPIOID_ER_LA_FLAG",
                 "MARKET_NEW_FLAG","QUANTITY","DAYS_SUPPLY", "PATIENT_PAY",
                 "IQVIA_PRESCRIBER_ID","MODEL_TYP_CD", "NPI", "DAYS_SINCE_LAST_OPIOID",
                 "PATIENT_BRTH_YR", "PATIENT_GENDER", "PAT_DX_AVAIL_FLAG", 
                 "MKTED_PROD_NM", "MKTED_PROD_FORMLTN_NM", "ROUTE_ADM_NM", "DOSAGE_FORM_NM", "UNIFM_PROD_NM", "USC_CD", #these are columns from the product reference table
                 "PRODNME", "GENNME", "MASTER_FORM", "CLASS", "DRUG", "LONGSHORTACTING",
                 "DEACLASSCODE", "STRENGTH_PER_UNIT", "UOM", "MME_CONVERSION_FACTOR",
                 "MME_CONVERSION_FACTOR_N", "MOUD", "INGESTION_DATE"]

# COMMAND ----------

df_opioid = (
    opioid_bupe_df
    .join(opioid_mkt_def_file.select(["NDC", "MARKET_NAME", "MOUD"]), "NDC", "left")
    .where(col('MARKET_NAME') == "O")
    .select(opioid_cols)
)
display(df_opioid.limit(20))

# COMMAND ----------

#write delta table to gold folder
df_opioid = (
    df_opioid
    .withColumn("UPDATE_DATE", date_format(current_date(),"yyyy-MM-dd"))
)

merge_condition = "tgt.CLAIM_ID = src.CLAIM_ID AND tgt.YEAR = src.YEAR" 
market_path = "opioid"
merge_delta_data(df_opioid, market_path, arei_gold_path, merge_condition, "YEAR")

#Write csv file to be exported to Shared Drive
#drop columns association with the product reference table
(
    df_opioid
    .drop("MKTED_PROD_NM", "MKTED_PROD_FORMLTN_NM", "ROUTE_ADM_NM", "DOSAGE_FORM_NM", "UNIFM_PROD_NM", "USC_CD")
    .repartition(1)
    .write
    .option("header",True) 
    .mode("overwrite")
    .csv(f"{csv_exports_path}/LRx/{market_path}/{year}/")
)

# COMMAND ----------

opioid = spark.read.format("delta").load(f"{arei_gold_path}/opioid")
display(opioid.groupBy("YEAR").count().orderBy("YEAR"))

# COMMAND ----------

