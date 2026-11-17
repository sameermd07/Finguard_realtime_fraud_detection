# Pipelines

Source files for the two Lakeflow Spark Declarative Pipelines (SDP) in this project.

| Pipeline | Folder | Mode | Purpose |
| --- | --- | --- | --- |
| `finguard-streaming` | `streaming/` | Triggered while developing, Continuous for the live demo | Kafka and file streams to bronze, silver, gold, plus email alert sinks |
| `finguard customers silver ingestion` | `customers_silver/` | Triggered (run daily by a Lakeflow Job) | `bronze.customers` to `silver.customers` |

In Databricks, create each pipeline and include its folders (`bronze`, `silver`, `gold`, `alert`) as pipeline source code. An excluded folder is skipped at run time.

**Things to check against your own data**

- `streaming/silver/transactions_silver.py`: the `schema` must match the JSON your Kafka producer sends.
- `streaming/bronze/fraud_watch_list_bronze.py` and `silver/fraud_watch_list_silver.py`: column names must match your `fraud_watchlist.csv`.
- `streaming/alert/email_notifier.py`: set `FROM` to the Gmail account that created the app password.
- `customers_silver/silver/customers_silver.py` and `fraud_watch_list_silver.py`: add a date or timestamp format string if your values are not ISO formatted.
