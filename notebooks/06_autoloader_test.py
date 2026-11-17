# Databricks notebook source
import pyspark.sql.functions as f

source_path = "/Volumes/finguard/source/fraud_watch_list/source_data/"
display(dbutils.fs.ls(source_path))        # confirm the files are visible

input_stream = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/finguard/source/fraud_watch_list/schema/")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path))

transformed_df = input_stream.select("*",
    f.col("_metadata.file_path").alias("file_path"),
    f.current_timestamp().alias("injection_timestamp"))

(transformed_df.writeStream.format("delta").outputMode("append")
  .option("checkpointLocation", "/Volumes/finguard/source/fraud_watch_list/checkpoint/")
  .trigger(availableNow=True)
  .toTable("finguard.bronze.fraud_watch_list_batch_test"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.fraud_watch_list_batch_test;
