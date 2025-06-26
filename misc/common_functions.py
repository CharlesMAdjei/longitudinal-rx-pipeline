# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def check_columns(expected_columns, actual_columns):
    """
    Check for missing and extra columns between expected and actual columns.

    :param expected_columns: List of expected column names.
    :param actual_columns: List of actual column names.
    """
    missing_columns = list(set(expected_columns) - set(actual_columns))
    extra_columns = list(set(actual_columns) - set(expected_columns))

    # Print results
    if missing_columns:
        print("❌ Missing columns:", missing_columns)
    if extra_columns:
        print("⚠️ Extra columns in current data not in expected columns:", extra_columns)
    if not missing_columns and not extra_columns:
        print("✅ Column check passed. All expected columns are present.")

# COMMAND ----------

lrx_patients_lookup_expected_columns = [
    "PATIENT_ID_REF",
    "PATIENT_BRTH_YR", 
    "PATIENT_GENDER", 
    "PAT_DX_AVAIL_FLAG",
    "ingestion_date"
]

# COMMAND ----------

lrx_product_lookup_expected_columns = [
    "NDC_CD",
    "MKTED_PROD_NM", 
    "MKTED_PROD_FORMLTN_NM", 
    "ROUTE_ADM_NM",
    "DOSAGE_FORM_NM",
    "UNIFM_PROD_NM",
    "USC_CD",
    "ingestion_date"
]

# COMMAND ----------

lrx_expected_columns = [
    "MARKET_NAME", 
    "PATIENT_ID",
    "CLAIM_ID",
    "RX_TYPE", 
    "NDC", 
    "CHNL_CD", 
    "PROVIDER_ZIP", 
    "PROVIDER_STATE", 
    "PROVIDER_ST_CNTY_FIPS_CD", 
    "PROVIDER_SPCL_CD",
    "DISPENSE_DATE", 
    "MONTH_ID", 
    "OPIOID_LA_SA_FLAG", 
    "MARKET_NEW_FLAG",
    "QUANTITY", 
    "DAYS_SUPPLY", 
    "PATIENT_PAY", 
    "IQVIA_PRESCRIBER_ID", 
    "MODEL_TYP_CD", 
    "NPI", 
    "DAYS_SINCE_LAST_OPIOID",
    "YEAR",
    "ingestion_date"
]

# COMMAND ----------

from pyspark.sql.functions import col

def check_row_count(manifest_df, patient_ref_df, data_type):
    """
    Check if the row count of the patient reference DataFrame matches the expected count from the manifest DataFrame.

    Parameters:
    manifest_df (DataFrame): DataFrame containing the expected record counts.
    patient_ref_df (DataFrame): DataFrame containing the actual patient records.

    Returns:
    None
    """
    # Get the expected count from the manifest DataFrame, casting to int
    expected_row = manifest_df.filter(col("DATA_TYPE") == f"{data_type}").select(col("RECORD_COUNT").cast("int")).collect()
    
    if not expected_row:
        print("❌ No expected row found for data type ")
        return
    
    expected_count = expected_row[0]["RECORD_COUNT"]  
    actual_count = patient_ref_df.count()+1  # Add 1 to account for the header row in manifest counts

    if expected_count == actual_count:
        print(f"✅ Row count matches: {actual_count}")
    else:
        print(f"❌ Row count does not match: {actual_count} != {expected_count}")

# COMMAND ----------

def calculate_missing_rows(df):
    # Count the number of missing rows for each column
    missing_rows = df.select([count(when(col(column).isNull(),1)).alias(column) for column in df.columns])
    
    return display(missing_rows)

# COMMAND ----------

from delta.tables import DeltaTable

def merge_delta_data(input_df, target_data_file, folder_path, merge_condition, partition_column, num_partitions=8):
    # Set dynamicPartitionPruning for optimization
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    # Check if target table is a Delta Lake table
    target_path = f"{folder_path}/{target_data_file}"
    if DeltaTable.isDeltaTable(spark, target_path):
        deltaTable = DeltaTable.forPath(spark, target_path)
        
        # Perform merge operation using merge function of DeltaTable
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        if num_partitions < 1:
            raise ValueError("Number of partitions must be greater than or equal to 1.")
        
        # Repartition the input DataFrame based on the provided number of partitions
        input_df = input_df.repartition(num_partitions)
        
        # Write input_df to a new Delta Lake table
        input_df.write.mode("overwrite").format("delta").save(target_path)


# COMMAND ----------

# UPSET function for incremental loading - This function ensure that the notebooks are re-runable without having duplicates 
# when it is run multiple times for the same data  file and the same year.
# def merge_delta_data_other(input_df, target_data_file, folder_path, merge_condition):

#     from delta.tables import DeltaTable
#     if (DeltaTable.isDeltaTable(spark, f"{folder_path}/{target_data_file}")):
#         deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{target_data_file}")
#         deltaTable.alias("tgt").merge(
#             input_df.alias("src"),
#             merge_condition) \
#         .whenMatchedUpdateAll()\
#         .whenNotMatchedInsertAll()\
#         .execute()
#     else:
#         input_df.write.mode("overwrite").format("delta").save(f"{folder_path}/{target_data_file}")

# COMMAND ----------


def merge_delta_data_other(input_df, target_data_file, folder_path, merge_condition, num_partitions=8):
    target_path = f"{folder_path}/{target_data_file}"
    if DeltaTable.isDeltaTable(spark, target_path):
        deltaTable = DeltaTable.forPath(spark, target_path)
        deltaTable.alias("tgt").merge(input_df.alias("src"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        if num_partitions < 1:
            raise ValueError("Number of partitions must be greater than or equal to 1.")
        
        input_df = input_df.repartition(num_partitions)  # Repartition the DataFrame based on the provided number of partitions
        input_df.write.mode("overwrite").format("delta").save(target_path)

# COMMAND ----------

def cleanup_delta_files(delta_path, retention):
    """
    Clean up unused Delta files using VACUUM.
    """
    spark.sql(f"VACUUM {delta_path} RETAIN {retention} HOURS")

# COMMAND ----------

def calculate_summary_statistics(data_frame, numerical_column, group_column):
    """
    Calculate the five basic summary statistics for a numerical column grouped by another column.

    Parameters:
        data_frame (DataFrame): The input DataFrame.
        numerical_column (str): The name of the numerical column for which statistics are to be calculated.
        group_column (str): The name of the column to group the DataFrame by.

    Returns:
        DataFrame: A DataFrame containing the five basic summary statistics for the numerical column
                   grouped by the group_column.
    """
    summary_statistics = data_frame.groupBy(group_column).agg(
        avg(numerical_column).alias("mean"),
        stddev(numerical_column).alias("stddev"),
        min(numerical_column).alias("min"),
        max(numerical_column).alias("max"),
        count(numerical_column).alias("count")
    ).orderBy(group_column)

    return summary_statistics

# COMMAND ----------


def count_outliers(data_frame, numerical_column, group_column):
    """
    Count the number of outliers in a numerical column of a DataFrame based on the upper quartile, lower quartile,
    and interquartile range (IQR), grouped by another column.

    Parameters:
        data_frame (DataFrame): The input DataFrame.
        numerical_column (str): The name of the numerical column to count outliers from.
        group_column (str): The name of the column to group the DataFrame by.

    Returns:
        DataFrame: A DataFrame containing the count of outliers for each group defined by the group_column.
    """
    # Calculate quartiles and IQR for the numerical column
    quartiles = data_frame.approxQuantile(numerical_column, [0.25, 0.75], 0.01)
    q1 = quartiles[0]
    q3 = quartiles[1]
    iqr = q3 - q1

    # Calculate lower and upper bounds for outliers
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    # Filter the DataFrame to get the rows that are outliers based on the calculated bounds
    outlier_condition = (F.col(numerical_column) < lower_bound) | (F.col(numerical_column) > upper_bound)
    outlier_count_df = data_frame.select(group_column).where(outlier_condition).groupBy(group_column).count()

    # Rename the count column to 'outlier_count'
    outlier_count_df = outlier_count_df.withColumnRenamed("count", "outlier_count")

    return outlier_count_df


# COMMAND ----------

def cal_summary_stats_with_outliers(data_frame, numerical_column, group_column):
    """
    Calculate summary statistics for a numerical column grouped by another column,
    and count the number of outliers based on the interquartile range (IQR).

    Parameters:
        data_frame (DataFrame): The input DataFrame.
        numerical_column (str): The name of the numerical column for which statistics are to be calculated.
        group_column (str): The name of the column to group the DataFrame by.

    Returns:
        DataFrame: A DataFrame containing summary statistics, outlier counts, percentage of outliers,
                   and total observations for each group.
    """
    # Constants
    APPROX_QUANTILE_ERROR = 0.01

    # Calculate quartiles and IQR for the numerical column
    quartiles = data_frame.approxQuantile(numerical_column, [0.25, 0.5, 0.75], APPROX_QUANTILE_ERROR)
    lower_quartile, median, upper_quartile = quartiles
    interquartile_range = upper_quartile - lower_quartile

    # Calculate lower and upper bounds for outliers
    lower_bound = lower_quartile - 1.5 * interquartile_range
    upper_bound = upper_quartile + 1.5 * interquartile_range

    # Create column expressions for aggregations
    count_outliers_expr = (
        sum(
            when(
                (col(numerical_column) < lower_bound) | (col(numerical_column) > upper_bound),
                1
            ).otherwise(0)
        ).alias("count_outliers")
    )

    total_observations_expr = count(numerical_column).alias("total_observations")

    # Calculate summary statistics, count of outliers, and total observations
    summary_statistics = data_frame.groupBy(group_column).agg(
        total_observations_expr,
        min(numerical_column).alias("min"),
        # expr(f"percentile_approx({numerical_column}, 0.25)").alias("q1"),
        expr(f"percentile_approx({numerical_column}, 0.5)").alias("median"),
        # expr(f"percentile_approx({numerical_column}, 0.75)").alias("q3"),
        round(avg(numerical_column), 2).alias("mean"),
        round(stddev(numerical_column), 2).alias("stddev"),
        max(numerical_column).alias("max"),
        count_outliers_expr
    )

    # Calculate percentage of outliers and round to 2 decimal places
    percentage_outliers_expr = (
        round((col("count_outliers") / col("total_observations") * 100), 2).alias("percentage_outliers")
    )

    # Add percentage of outliers to the summary statistics DataFrame
    summary_statistics_with_percentage = summary_statistics.withColumn(
        "percentage_outliers", percentage_outliers_expr
    )

    return summary_statistics_with_percentage