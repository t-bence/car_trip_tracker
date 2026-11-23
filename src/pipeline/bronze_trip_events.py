"""Trip events as the ingest Lambda landed them in S3.

The Lambda writes one JSON object per event, so this is an Auto Loader stream
over that prefix. The schema is declared rather than inferred, and anything
that does not fit it is kept in `_rescued_data` instead of being lost.
"""

from pyspark import pipelines as dp

EVENTS_PATH = spark.conf.get("events_path")  # noqa: F821

EVENT_SCHEMA = """
    event_id STRING,
    event STRING,
    event_time STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    logged_at STRING,
    received_at STRING,
    _rescued_data STRING
"""


@dp.table(comment="Trip events exactly as the ingest Lambda wrote them to S3")
def bronze_trip_events():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .schema(EVENT_SCHEMA)
        .load(EVENTS_PATH)
    )
