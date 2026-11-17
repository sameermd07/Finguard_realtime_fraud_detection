# Databricks notebook source
# NOTE: replace the <PLACEHOLDERS> below with your real values ONLY inside Databricks.
# Never commit real keys to Git. Clear the values from the cell after running it.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
scope = "finguard-scope"

try:
    w.secrets.create_scope(scope=scope)
    print("Scope created")
except Exception as e:
    print("Scope not created (it may already exist):", e)

# COMMAND ----------

import json

kafka = {
    "bootstrap_servers": "<CONFLUENT_BOOTSTRAP_SERVER>:9092",
    "topic": "credit_card_transactions",
    "api_key": "<CONFLUENT_API_KEY>",
    "api_secret": "<CONFLUENT_API_SECRET>",
}

w.secrets.put_secret(scope="finguard-scope",
                     key="kafka-connection-details",
                     string_value=json.dumps(kafka))
print("Secret saved")

# COMMAND ----------

cfg = json.loads(dbutils.secrets.get(scope="finguard-scope", key="kafka-connection-details"))
print(cfg["topic"])
print(cfg["bootstrap_servers"].endswith(":9092"))
