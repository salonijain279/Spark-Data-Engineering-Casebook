"""Build analytics-ready Yelp tables from a mixed JSONL export."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F


def build_silver(raw: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Split mixed records and retain all reviews in the silver join."""
    reviews = raw.filter(F.col("type") == "review")
    users = raw.filter(F.col("type") == "user")
    businesses = raw.filter(F.col("type") == "business")

    user_lookup = users.select(
        "user_id",
        F.col("name").alias("user_name"),
        F.col("average_stars").alias("user_average_stars"),
        F.col("review_count").alias("user_review_count"),
    )
    business_lookup = businesses.select(
        "business_id",
        F.col("name").alias("business_name"),
        F.col("city").alias("business_city"),
        F.col("state").alias("business_state"),
        F.col("stars").alias("business_average_stars"),
    )

    silver = (
        reviews.alias("r")
        .join(user_lookup.alias("u"), on="user_id", how="left")
        .join(business_lookup.alias("b"), on="business_id", how="left")
    )
    return reviews, users, businesses, silver


def write_tables(raw: DataFrame, output: str) -> None:
    reviews, users, businesses, silver = build_silver(raw)
    tables = {
        "bronze": raw,
        "reviews": reviews,
        "users": users,
        "businesses": businesses,
        "reviews_silver": silver,
    }
    for name, frame in tables.items():
        frame.write.mode("overwrite").parquet(f"{output}/{name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Yelp JSONL file or glob")
    parser.add_argument("--output", required=True, help="Output directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("yelp-medallion-pipeline").getOrCreate()
    raw = spark.read.json(args.input)
    write_tables(raw, args.output.rstrip("/"))
    spark.stop()


if __name__ == "__main__":
    main()
