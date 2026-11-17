# Concepts cheat sheet

## Streaming vs batch
Batch collects data for an hour or a day and then processes it. Streaming pushes each event through as it is generated, with latency of seconds or less. Use batch when delay is acceptable and volumes are large; use streaming for fraud, IoT and live dashboards.

## Kafka vocabulary

| Term | Meaning |
| --- | --- |
| Producer | Anything that publishes messages (app, microservice, script) |
| Cluster and broker | The Kafka servers; brokers are the nodes that store the data |
| Topic | A named channel for messages, like a table (`credit_card_transactions`) |
| Partition | A topic is split into partitions (6 by default on Confluent) so data is written and read in parallel |
| Offset | Position of a message inside one partition; consumers track the last offset read |
| Consumer group | Consumers in one group split the partitions; different groups each get a full copy |
| Key and value | Every message has a key (the transaction ID) and a value (the JSON payload); both arrive as binary |

## Stateless vs stateful processing
- **Stateless:** each event is processed alone. Filters, casts, and enrichment with a static table (stream-static join). Cheap and low latency.
- **Stateful:** needs memory of earlier events. Windowed aggregations, stream-stream joins, running totals. Needs more resources, which is why watermarks exist.

## Watermarks
A watermark says how late an event may arrive. Watermark = latest event time seen minus the threshold. With a 10-minute watermark and a latest event at 12:30, anything older than 12:20 is ignored. It is based on the data's own timestamps, not the clock, and it bounds the state Spark keeps in memory.

## Windows

| Type | Parameters | Behaviour |
| --- | --- | --- |
| Tumbling | size | Fixed, non-overlapping; each event falls in exactly one window |
| Sliding | size and slide | Windows overlap; an event can fall into several (size 5 min, slide 1 min: five windows) |

## Joins on streams
- **Stream-static:** use a **left** join and read the static table with `spark.read.table` (not `readStream`).
- **Stream-stream:** use an **inner** join; Spark only allows it with watermarks on both streams.

## Triggers and pipeline modes
- `trigger(availableNow=True)`: process everything new, then stop. Works on Databricks Free Edition notebooks.
- `trigger(processingTime=...)`: continuous micro-batches. Not supported on Free Edition serverless.
- In SDP, use pipeline modes instead: **Triggered** (run once) or **Continuous**.

## Medallion layers
- **Bronze:** raw data exactly as received, plus ingestion metadata.
- **Silver:** parsed, typed and validated data (data quality rules drop or flag bad rows).
- **Gold:** business-ready outputs: alerts and aggregations.
