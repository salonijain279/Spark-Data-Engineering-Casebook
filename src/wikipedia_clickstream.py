"""Create page traffic and navigation-flow metrics from Wikipedia clickstream TSV."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


SCHEMA = T.StructType(
    [
        T.StructField("prev_id", T.LongType()),
        T.StructField("curr_id", T.LongType()),
        T.StructField("prev_title", T.StringType()),
        T.StructField("curr_title", T.StringType()),
        T.StructField("hits", T.LongType()),
        T.StructField("link_type", T.StringType()),
    ]
)


def aggregate_clickstream(df: DataFrame) -> dict[str, DataFrame]:
    top_pages = (
        df.groupBy(F.col("curr_title").alias("page_title"))
        .agg(F.sum("hits").alias("total_hits"))
        .orderBy(F.desc("total_hits"))
    )
    top_referrers = (
        df.groupBy(F.col("prev_title").alias("referrer"))
        .agg(F.sum("hits").alias("total_hits"))
        .orderBy(F.desc("total_hits"))
    )

    incoming = df.groupBy("curr_id", "curr_title").agg(F.sum("hits").alias("in_count"))
    outgoing = (
        df.filter(F.col("prev_id").isNotNull())
        .groupBy("prev_id", "prev_title")
        .agg(F.sum("hits").alias("out_count"))
    )
    page_flows = (
        incoming.alias("i")
        .join(outgoing.alias("o"), F.col("i.curr_id") == F.col("o.prev_id"), "full")
        .select(
            F.coalesce(F.col("i.curr_id"), F.col("o.prev_id")).alias("page_id"),
            F.coalesce(F.col("i.curr_title"), F.col("o.prev_title")).alias("page"),
            F.coalesce(F.col("i.in_count"), F.lit(0)).alias("in_count"),
            F.coalesce(F.col("o.out_count"), F.lit(0)).alias("out_count"),
        )
        .withColumn(
            "out_in_ratio",
            F.when(F.col("in_count") > 0, F.col("out_count") / F.col("in_count")),
        )
    )
    return {"top_pages": top_pages, "top_referrers": top_referrers, "page_flows": page_flows}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--sample", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("wikipedia-clickstream-metrics").getOrCreate()
    df = spark.read.csv(args.input, sep="\t", schema=SCHEMA)
    if args.sample < 1.0:
        df = df.sample(withReplacement=False, fraction=args.sample, seed=1234)
    for name, frame in aggregate_clickstream(df).items():
        frame.write.mode("overwrite").parquet(f"{args.output.rstrip('/')}/{name}")
    spark.stop()


if __name__ == "__main__":
    main()
