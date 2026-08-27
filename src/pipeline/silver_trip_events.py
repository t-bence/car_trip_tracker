"""Current state of trip start/end events.

Assumes Lakehouse Sync (configured manually in the UI - see README) targets
this pipeline's catalog/schema, so the CDC history table can be read by its
bare name. Lakebase only stores a raw JSON `payload` TEXT cell plus the
insert timestamp, so this layer is also where that JSON gets parsed.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    comment="Current state of trip start/end events, deduplicated from the "
    "Lakebase CDC history table and parsed from their JSON payload",
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
@dp.expect_or_drop("valid_event_type", "event_type IN ('start', 'end')")
def silver_trip_events():
    history = spark.read.table("lb_trip_events_history").where(
        F.col("_pg_change_type").isin("insert", "update_postimage", "delete")
    )
    latest = (
        history.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("id").orderBy(F.col("_pg_lsn").desc())
            ),
        )
        .where((F.col("rn") == 1) & (F.col("_pg_change_type") != "delete"))
    )
    return latest.select(
        "id",
        F.get_json_object("payload", "$.event_type").alias("event_type"),
        F.get_json_object("payload", "$.note").alias("note"),
        F.col("received_at").alias("event_time"),
    )
