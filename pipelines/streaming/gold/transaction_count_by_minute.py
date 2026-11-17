# Gold: transactions per 1-minute TUMBLING window (fixed size, no overlap).
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


@dp.table(
    name="finguard.gold.transaction_count_by_minute",
    comment="Transaction count per 1-minute tumbling window",
)
def transaction_count_by_minute() -> DataFrame:
    transactions = spark.readStream.table("finguard.silver.transactions")
    tx_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")

    df = (
        tx_wm.groupBy(f.window("transaction_timestamp", "1 minute"))
        .agg(f.count("*").alias("transaction_count"))
        .select(
            f.col("window.start").alias("window_start"),
            f.col("window.end").alias("window_end"),
            "transaction_count",
        )
    )
    return df
