# Gold rule 2: FRAUD WATCH LIST MATCH (stream-stream inner join).
# A transaction on a card that appears on the watch list within the watermark window.
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


@dp.table(
    name="finguard.gold.fraud_card_alerts",
    comment="Transactions on cards that appear on the fraud watch list",
)
def fraud_card_alerts() -> DataFrame:
    transactions = spark.readStream.table("finguard.silver.transactions")
    watch_list = spark.readStream.table("finguard.silver.fraud_watch_list")
    customers = spark.read.table("finguard.silver.customers")  # static lookup

    # Stream-stream joins need a watermark on BOTH sides (5 minutes is a demo value).
    tx_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")
    wl_wm = watch_list.withWatermark("effective_from", "5 minutes")

    fraud_detected = tx_wm.join(
        wl_wm, tx_wm.card_number == wl_wm.entity_id, "inner"
    ).join(customers, tx_wm.customer_id == customers.customer_id, "left")

    return fraud_detected.select(
        f.concat_ws("-", f.lit("FRAUD"), tx_wm.transaction_id, wl_wm.watchlist_id).alias("alert_id"),
        f.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
        f.current_timestamp().alias("alert_timestamp"),
        tx_wm.transaction_id,
        tx_wm.customer_id,
        customers.email.alias("customer_email"),
        f.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
        tx_wm.amount.alias("transaction_amount"),
        tx_wm.currency,
        tx_wm.merchant_name,
        tx_wm.merchant_category,
        tx_wm.transaction_type,
        tx_wm.payment_channel,
        tx_wm.city,
        tx_wm.country,
        tx_wm.is_international,
        tx_wm.transaction_timestamp,
        tx_wm.status,
        wl_wm.watchlist_id,
        wl_wm.watch_type,
        wl_wm.risk_level,
        wl_wm.action,
        wl_wm.reason_code,
        wl_wm.reason_description,
        wl_wm.effective_from.alias("watch_list_effective_from"),
        wl_wm.city.alias("watch_list_city"),
        wl_wm.country.alias("watch_list_country"),
    )
