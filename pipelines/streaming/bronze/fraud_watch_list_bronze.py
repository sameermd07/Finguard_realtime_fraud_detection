# Bronze: fraud watch list JSON files ingested with Auto Loader (cloudFiles).
# NOTE: column names must match your fraud_watchlist.csv / generated JSON files.
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


@dp.table(
    name="finguard.bronze.fraud_watch_list",
    comment="Raw fraud watch list ingested by Auto Loader",
)
def fraud_watch_list_bronze() -> DataFrame:
    streaming_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/finguard/source/fraud_watch_list/source_data")
    )  # no schemaLocation needed inside SDP

    # Listing columns fixes their order (Auto Loader does not guarantee JSON column order).
    return streaming_df.select(
        "watchlist_id",
        "watch_type",
        "entity_id",
        "risk_level",
        "action",
        "reason_code",
        "reason_description",
        "status",
        "effective_from",
        "reported_by",
        "reported_source",
        "country",
        "city",
        "_rescued_data",
        f.col("_metadata.file_path").alias("source_file"),
        f.current_timestamp().alias("injection_timestamp"),
    )
