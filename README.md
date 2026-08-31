# Spark Data Engineering: From Raw Files to Decision-Ready Tables

This casebook presents four Big Data coursework builds as standalone, reusable data-engineering projects. Together they cover semi-structured Yelp records, Wikipedia clickstream traffic, airline route activity, and a Hive/Delta-style table lifecycle. Each project is framed around a distinct data problem, pipeline design, and decision-ready output.

## What this demonstrates

- Reading large JSON, TSV, and CSV inputs with explicit schemas where possible
- Separating raw records into business entities and rebuilding an analytics-ready silver table
- Aggregating navigation flows with full-outer joins and safe ratio calculations
- Converting row-oriented source files into partition-friendly Parquet outputs
- Writing idempotent jobs with explicit CLI inputs and overwrite semantics

## Project portfolio

| Pipeline | Business question | Build |
|---|---|---|
| Project 1 — Yelp medallion pipeline | How can mixed review, user, and business records become a reusable analytics table? | Bronze ingestion, entity separation, left joins, silver Parquet output |
| Project 2 — Wikipedia clickstream | Which pages attract traffic, which send it onward, and where are navigation imbalances? | Explicit schema, top-page and referrer metrics, full-outer page flow table |
| Project 3 — Flight route metrics | Which origin-destination routes carry the most activity? | Multi-file CSV ingestion, route aggregation, columnar output |
| Project 4 — Hive/Delta table lifecycle | How does a raw external source become a managed, query-efficient analytics table? | External CSV table, managed Parquet table, route query, rerun-safe cleanup |

## Repository structure

```text
src/
  yelp_medallion.py
  wikipedia_clickstream.py
  flight_route_metrics.py
sql/
  flight_table_lifecycle.sql
tests/
  test_source_contracts.py
```

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

spark-submit src/yelp_medallion.py --input /path/to/yelp.json --output outputs/yelp
spark-submit src/wikipedia_clickstream.py --input /path/to/clickstream.tsv --output outputs/wiki
spark-submit src/flight_route_metrics.py --input '/path/to/flights/*.csv' --output outputs/flights
```

The code is compatible with local Spark and can be adapted to Databricks Volumes by passing `/Volumes/...` paths. Public dataset references are documented in each module; large source files are intentionally not committed.

## Design notes

- Jobs use deterministic column selection and explicit output locations.
- Derived tables are overwritten atomically by Spark, making reruns predictable.
- The Yelp join keeps every review even when related user or business rows are missing.
- The Wikipedia page-flow result retains pages that appear only as a source or only as a destination.

## Origin

Rebuilt from MSBA Big Data Analytics coursework as a portable portfolio project. Classroom prompts, exam material, and bulky notebook exports are excluded.
