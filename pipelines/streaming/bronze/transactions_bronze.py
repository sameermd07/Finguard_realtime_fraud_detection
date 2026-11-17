# Bronze: raw card transactions streamed from Confluent Kafka.
# Lakeflow Spark Declarative Pipelines (SDP) manages checkpoints and triggers.
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import json


@dp.table(
    name="finguard.bronze.transactions",
    comment="Raw transaction stream data ingested from Kafka",
)
def transactions_bronze() -> DataFrame:
    cfg = json.loads(
        dbutils.secrets.get(scope="finguard-scope", key="kafka-connection-details")
    )
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f"required username='{cfg['api_key']}' password='{cfg['api_secret']}';"
    )

    streaming_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["bootstrap_servers"])
        .option("subscribe", cfg["topic"])
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("startingOffsets", "earliest")
        .load()
    )

    return (
        streaming_df.select(
            f.col("key").cast("string"),
            f.col("value").cast("string"),
            "topic",
            "partition",
            "offset",
            "timestamp",
        ).withColumn("injection_timestamp", f.current_timestamp())
    )
