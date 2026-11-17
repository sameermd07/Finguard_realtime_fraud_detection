# Silver customers: runs in its OWN pipeline so the daily batch load does not run
# continuously alongside the streaming pipeline. Reads bronze.customers (Lakeflow Connect).
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


@dp.table(
    name="finguard.silver.customers",
    comment="Parsed and cleaned customers data",
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customers_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.customers")
    return (
        # Add a format string as the 2nd argument if dates are not yyyy-MM-dd.
        bronze_df.withColumn("account_open_date", f.to_date("account_open_date"))
        .withColumn("silver_injection_timestamp", f.current_timestamp())
    )
