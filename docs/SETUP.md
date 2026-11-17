# Setup guide: build FinGuard from scratch

Follow the steps in order. Every step ends with a check. Everything here runs on free tiers; no credit card is needed.

## Step 0: Accounts and tools

| Account | Used for |
| --- | --- |
| Databricks Free Edition | Notebooks, pipelines, jobs, dashboard, Unity Catalog |
| Confluent Cloud | Kafka topic (a promo code avoids entering a card) |
| Neon | Postgres customer table |
| Google account | Gmail App Password (2-step verification must be on) |

Install locally: **Python 3**, **Visual Studio Code** with the Python extension, and Git.

Create the virtual environment for the Kafka producer scripts (VS Code: open the producer folder, then Command Palette, **Python: Create Environment**, **Venv**, **Quick Create**), or manually:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt  # the requirements file that ships with the producer scripts
```

**Check:** `python --version` works and the terminal prompt starts with `(.venv)`.

## Step 1: Kafka on Confluent Cloud

1. Create a cluster (basic, nearest region).
2. Create the topic `credit_card_transactions` (6 partitions, default 1-week retention).
3. Copy the **bootstrap server** (ends in `:9092`) and create an **API key and secret**. The secret is shown only once.
4. In the producer folder, edit `.env` and fill in the bootstrap server, API key, API secret and topic. `.env` is git-ignored, so it never reaches GitHub.
5. Run `python producer_normal.py` for a few seconds, then Ctrl+C.

The producer scripts (`producer_normal.py`, `producer_fraud_transaction.py`, `producer_fraud_card.py`) come from the tutorial author's repository and are not redistributed here.

**Check:** the topic shows messages in the Confluent console.

## Step 2: Databricks workspace, catalog and secrets

1. Import the notebooks from `notebooks/` into a workspace folder (Workspace, Import), or add this repo as a Git folder.
2. Run `00_setup_catalog_and_volumes.py`. It creates catalog `finguard`, schemas `source`, `bronze`, `silver`, `gold`, two volumes, and the checkpoint folders.
3. Open `01_set_up_secrets.py`, replace the `<PLACEHOLDERS>` with your real Confluent values, run it, then **clear the values from the cell**. It creates scope `finguard-scope` and the secret `kafka-connection-details` (one JSON with `bootstrap_servers`, `topic`, `api_key`, `api_secret`).

**Check:** the last cell prints `credit_card_transactions` and `True`.

## Step 3: Spark Structured Streaming test

Run `02_kafka_streaming_test.py`. It reads Kafka as a batch (`spark.read`), then as a stream (`spark.readStream`), and writes `finguard.bronze.source_test` with a checkpoint and `availableNow=True`.

**Check:** a second run, after sending more messages with the producer, appends only the new records.

## Step 4: SDP pipeline, bronze and silver transactions

1. Create an **ETL pipeline** `finguard-streaming`, default catalog `finguard`, default schema `bronze`.
2. Add the folders `bronze`, `silver`, `gold`, `alert` from `pipelines/streaming/` and include each as pipeline source code.
3. Start with `bronze/transactions_bronze.py` and `silver/transactions_silver.py`, then run the pipeline.

**Check:** the graph shows bronze then silver, the five data quality rules are met, and `finguard.silver.transactions` has one column per JSON field.

## Step 5: Customers from Postgres (Neon) with Lakeflow Connect

1. Create a Neon project `finguard`, run the historic-load SQL script from the tutorial repo (1,000 customers; set your own email), and copy the connection string.
2. In Databricks create an **Ingestion pipeline** (PostgreSQL): connection `finguard-postgres`, database `neondb`, schema `public`, table `customers`, cursor column `update_timestamp`, primary key `customer_id`, destination `finguard.bronze`. Remove the default schedule.
3. Create the second ETL pipeline `finguard customers silver ingestion` from `pipelines/customers_silver/` and run it.

**Check:** `finguard.silver.customers` exists with `account_open_date` as a date. Running the tutorial's incremental SQL and re-running ingestion updates customer 1 and adds customers 101 and 102.

## Step 6: High-value alert and Gmail alerts

1. Add `gold/high_value_transactions_alert.py` (stream-static left join on `silver.customers`).
2. Create a Gmail **App Password**, store it with `put_secret` under key `gmail-app-password`, and test with `03_email_test.py`.
3. Set `FROM` in `alert/email_notifier.py` and include the `alert` folder as source code.
4. Run `python producer_fraud_transaction.py` (one transaction above the customer limit), then run the pipeline.

**Check:** one row in `gold.high_value_transactions_alert` and an email in your inbox (check Spam).

## Step 7: File stream with Auto Loader (fraud watch list)

1. Upload `fraud_watchlist.csv` to the volume `finguard.source.fraud_watch_list`.
2. Run `05_fraud_watchlist_data_generator.py`: it writes one JSON file every 5 seconds into `source_data/`.
3. Optionally explore Auto Loader with `06_autoloader_test.py`.
4. Add `bronze/fraud_watch_list_bronze.py` and `silver/fraud_watch_list_silver.py` and run the pipeline.

**Check:** `silver.fraud_watch_list` has one row per file, IDs upper-cased, `effective_from` as a timestamp.

## Step 8: Stream-stream join, fraud card alert

Add `gold/fraud_card_alerts.py` (inner join on `card_number = entity_id`, 5-minute watermark on both streams). Make sure `transaction_timestamp` is a `TimestampType` in the silver schema, then run a **Full refresh**.

**Check:** `gold.fraud_card_alerts` has rows and a second email arrives.

## Step 9: Window aggregations

Add `gold/transaction_count_by_minute.py` (tumbling, 1 minute) and `gold/transaction_count_sliding_window.py` (5 minutes, sliding every 1 minute). Output lags by about the watermark (5 minutes).

**Check:** tumbling windows are 1 minute apart; sliding windows start every minute and span 5 minutes.

## Step 10: Run it live, dashboard, orchestration

1. Set `finguard-streaming` to **Continuous**, run the watch-list generator and `producer_normal.py`, then `producer_fraud_transaction.py`.
2. Create an AI/BI dashboard over `finguard.silver` and `finguard.gold` (alerts over time, alert types, top customers, volume per minute) and publish it. Schedule it to refresh every 1 minute.
3. Create a Lakeflow Job that runs the customer ingestion pipeline, then the customers silver pipeline, scheduled daily.
4. When you are done, stop the pipeline, delete the Confluent cluster, and revoke the Confluent API key and Gmail app password.

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `display()` fails on a streaming DataFrame | Streaming frames cannot be displayed | Write to a table and query it |
| `processingTime` trigger rejected | Not supported on Free Edition serverless | Use `availableNow=True`, or SDP Continuous mode |
| Kafka read hangs, then times out | Wrong bootstrap port (for example `909`) or wrong key/secret | Fix the value and re-put the secret |
| "Secret does not exist" | Scope or key name typo | Names must match exactly |
| Pipeline stuck "waiting for resources" | Free Edition serverless is busy | Stop and retry later |
| `invalid syntax` at an import | Missing `import` keyword | `import pyspark.sql.functions as f` |
| `StructField` not found on `f` | Genie wrote `f.StructField` | Import from `pyspark.sql.types` |
| "Cannot move tables across catalogs" on silver customers | Free Edition quirk | Recreate the ETL pipeline, paste the code, default schema `bronze` |
| `upper()` invalid character error | Missing alias in the select | `f.upper("col").alias("col")` |
| Fraud-card table fails on timestamp type | `transaction_timestamp` stored as string | Use `TimestampType` and run a Full refresh |
| "returns NoneType" | Missing `return` in the function | Return the DataFrame |
| Continuous mode flashes an error and disappears | Real error hidden | Switch to Triggered to read it |
| `Column object is not callable` | Bracket misplaced around `groupBy` | Move the bracket |
| Emails not arriving | Gmail filters personal-address mail | Check Spam |
| Sample data will not load in Catalog Explorer | SQL warehouse busy | Query from a notebook instead |
