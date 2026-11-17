# Gold rule 1: HIGH VALUE TRANSACTION (stream-static left join).
# Alert when a transaction amount is greater than the customer's transaction_limit.
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


@dp.table(
    name="finguard.gold.high_value_transactions_alert",
    comment="Alerts where the transaction value is higher than the customer-configured limit",
)
def high_value_transactions_alert() -> DataFrame:
    transactions = spark.readStream.table("finguard.silver.transactions")  # stream
    customers = spark.read.table("finguard.silver.customers")  # static: read, NOT readStream

    joined = transactions.join(
        customers, transactions.customer_id == customers.customer_id, "left"
    ).filter(f.col("amount") > f.col("transaction_limit"))

    return joined.select(
        f.concat_ws("-", f.lit("ALERT"), transactions.transaction_id).alias("alert_id"),
        f.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
        f.current_timestamp().alias("alert_timestamp"),
        transactions.transaction_id,
        transactions.customer_id,
        customers.email.alias("customer_email"),
        f.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
        transactions.amount.alias("transaction_amount"),
        customers.transaction_limit,
        transactions.currency,
        transactions.merchant_name,
        transactions.merchant_category,
        transactions.transaction_type,
        transactions.payment_channel,
        transactions.city,
        transactions.country,
        transactions.is_international,
        transactions.transaction_timestamp,
        transactions.status,
    )
