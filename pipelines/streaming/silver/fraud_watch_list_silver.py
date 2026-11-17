# Silver: cleaned fraud watch list (upper-cased keys, typed timestamp).
# _rescued_data is intentionally left in bronze only (debugging column).
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


@dp.table(
    name="finguard.silver.fraud_watch_list",
    comment="Cleaned fraud watch list",
)
def fraud_watch_list_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.fraud_watch_list")
    return bronze_df.select(
        f.upper("watchlist_id").alias("watchlist_id"),
        "watch_type",
        f.upper("entity_id").alias("entity_id"),
        f.upper("risk_level").alias("risk_level"),
        f.upper("action").alias("action"),
        "reason_code",
        "reason_description",
        "status",
        # Add a format string as the 2nd argument if your values are not ISO timestamps.
        f.to_timestamp("effective_from").alias("effective_from"),
        "reported_by",
        "reported_source",
        "country",
        "city",
        "source_file",
        f.col("injection_timestamp").alias("bronze_injection_timestamp"),
        f.current_timestamp().alias("silver_injection_timestamp"),
    )
