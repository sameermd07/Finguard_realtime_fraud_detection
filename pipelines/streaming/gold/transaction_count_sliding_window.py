# Gold: transactions per 5-minute SLIDING window, advancing every 1 minute (windows overlap).
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


@dp.table(
    name="finguard.gold.transaction_count_sliding_window",
    comment="Transaction count per 5-minute sliding window (slide 1 minute)",
)
def transaction_count_sliding_window() -> DataFrame:
    transactions = spark.readStream.table("finguard.silver.transactions")
    tx_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")

    df = (
        # window(timeColumn, windowDuration, slideDuration)
        tx_wm.groupBy(f.window("transaction_timestamp", "5 minutes", "1 minute"))
        .agg(f.count("*").alias("transaction_count"))
        .select(
            f.col("window.start").alias("window_start"),
            f.col("window.end").alias("window_end"),
            "transaction_count",
        )
    )
    return df
