# Databricks notebook source
import json
from pyspark.sql.functions import col

cfg = json.loads(dbutils.secrets.get(scope="finguard-scope", key="kafka-connection-details"))
bootstrap, topic = cfg["bootstrap_servers"], cfg["topic"]
key, secret = cfg["api_key"], cfg["api_secret"]

jaas = ("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f"required username='{key}' password='{secret}';")

# COMMAND ----------

sample_batch = (spark.read.format("kafka")
  .option("kafka.bootstrap.servers", bootstrap)
  .option("subscribe", topic)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.sasl.jaas.config", jaas)
  .option("startingOffsets", "earliest")
  .load())

sample_batch.count()

# COMMAND ----------

display(sample_batch)

# COMMAND ----------

from pyspark.sql.functions import col;
display(sample_batch.select(col('value').cast('string')))

# COMMAND ----------

sample_batch_streaming = (spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", bootstrap)
  .option("subscribe", topic)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.sasl.jaas.config", jaas)
  .option("startingOffsets", "earliest")
  .load())

# COMMAND ----------

(sample_batch_streaming.writeStream
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation", "/Volumes/finguard/source/transactions/checkpoint/")
 .trigger(availableNow=True)
 .toTable("finguard.bronze.source_test")
)
