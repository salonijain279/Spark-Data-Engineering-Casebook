"""Aggregate multi-file flight records into route-level activity metrics."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


ROUTE_COLUMNS = ["ORIGIN_AIRPORT_ID", "ORIGIN", "DEST_AIRPORT_ID", "DEST", "DISTANCE"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="CSV file, directory, or glob")
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("flight-route-metrics").getOrCreate()
    flights = spark.read.option("header", True).option("inferSchema", True).csv(args.input)
    missing = sorted(set(ROUTE_COLUMNS) - set(flights.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    routes = (
        flights.groupBy(*ROUTE_COLUMNS)
        .agg(F.count(F.lit(1)).alias("num_flights"))
        .orderBy(F.desc("num_flights"))
    )
    routes.write.mode("overwrite").parquet(args.output)
    spark.stop()


if __name__ == "__main__":
    main()

