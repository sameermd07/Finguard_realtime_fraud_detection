# Databricks notebook source
# Run this once, before any other notebook. Creates the Unity Catalog objects FinGuard uses.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS finguard;
# MAGIC CREATE SCHEMA IF NOT EXISTS finguard.source;
# MAGIC CREATE SCHEMA IF NOT EXISTS finguard.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS finguard.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS finguard.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS finguard.source.transactions;      -- Kafka test checkpoint
# MAGIC CREATE VOLUME IF NOT EXISTS finguard.source.fraud_watch_list;  -- watch list files + Auto Loader state

# COMMAND ----------

# Folders used by the streaming test and Auto Loader notebooks
for path in [
    "/Volumes/finguard/source/transactions/checkpoint",
    "/Volumes/finguard/source/fraud_watch_list/source_data",
    "/Volumes/finguard/source/fraud_watch_list/schema",
    "/Volumes/finguard/source/fraud_watch_list/checkpoint",
]:
    dbutils.fs.mkdirs(path)
    print("ready:", path)
