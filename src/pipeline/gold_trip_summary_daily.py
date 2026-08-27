"""Daily trip counts and duration stats."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment="Daily trip counts and duration stats", cluster_by=["trip_date"])
def gold_trip_summary_daily():
    trips = spark.read.table("silver_trips")
    return (
        trips.withColumn("trip_date", F.to_date("start_time"))
        .groupBy("trip_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("duration_minutes").alias("total_duration_minutes"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.min("duration_minutes").alias("min_duration_minutes"),
            F.max("duration_minutes").alias("max_duration_minutes"),
        )
    )
