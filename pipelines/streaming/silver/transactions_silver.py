# Silver: parse the Kafka JSON payload into columns and apply data quality rules.
# NOTE: adjust the schema to match the JSON your producer actually sends.
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import pyspark.sql.functions as f

schema = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("card_number", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_type", StringType()),
        StructField("payment_channel", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("is_international", StringType()),
        StructField("transaction_timestamp", TimestampType()),
        StructField("status", StringType()),
    ]
)


@dp.table(
    name="finguard.silver.transactions",
    comment="Parsed and cleaned transactions data",
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")  # logs violations, does not drop rows
def transactions_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.transactions")
    return (
        bronze_df.select(
            f.from_json(f.col("value"), schema).alias("data"),
            f.col("topic").alias("kafka_topic"),
            f.col("partition").alias("kafka_partition"),
            f.col("offset").alias("kafka_offset"),
            f.col("timestamp").alias("kafka_timestamp"),
            f.col("injection_timestamp").alias("bronze_injection_timestamp"),
        )
        .select(
            "data.*",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "bronze_injection_timestamp",
        )
        .withColumn("silver_injection_timestamp", f.current_timestamp())
    )
