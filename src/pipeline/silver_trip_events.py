"""Trip start/stop events, typed and validated.

The Lambda has already unwrapped the payload and normalised the timestamp, so
this step only casts the landed strings and drops anything unusable.
"""

from pyspark import pipelines as dp
from transformations import parse_trip_event


@dp.table(comment="Trip start/stop events, typed")
@dp.expect_or_drop("valid_event_type", "event_type IN ('start', 'stop')")
@dp.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dp.expect_or_drop("valid_coordinates", "latitude IS NOT NULL AND longitude IS NOT NULL")
def silver_trip_events():
    events = spark.readStream.table("bronze_trip_events")  # noqa: F821
    return parse_trip_event(events)
