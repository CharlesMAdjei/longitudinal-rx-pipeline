# Databricks notebook source
# Path to ddnid-ncipc container
container = "abfss://ddnid-ncipc@edavsynapsedatalake.dfs.core.windows.net"

# COMMAND ----------

# Path to IO IQVIA acquired gold folder
oi_gold_path = f"{container}/OD/acquired-data/gold"

# COMMAND ----------

# Path to AREI gold database
arei_bronze_path = f"{container}/DOP/IQVIA/bronze/LRx"
arei_silver_path = f"{container}/DOP/IQVIA/silver/LRx"
arei_gold_path = f"{container}/DOP/IQVIA/gold/LRx"
csv_exports_path = f"{container}/DOP/IQVIA/gold/csv_exports"
arei_ws_path = f"{container}/DOP/IQVIA/workspace"

# COMMAND ----------

# Other files -- MME, Market definitions
other_file_path = f"{container}/DOP/IQVIA/bronze/other_files/"

# COMMAND ----------

