from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="silver_trips",
    comment="Cleaned, typed, deduplicated Citi Bike trips"
)
@dp.expect_or_drop("valid_ride_id",     "ride_id IS NOT NULL")
@dp.expect_or_drop("valid_times",       "started_at IS NOT NULL AND ended_at IS NOT NULL")
@dp.expect_or_drop("positive_duration", "ended_at > started_at")
@dp.expect("has_start_station",         "start_station_id IS NOT NULL")
@dp.expect("under_24_hours",            "duration_min <= 1440")
def silver_trips():
    return (
        spark.read.table("workspace.citibike.bronze_trips")
        .select(
            "ride_id",
            "rideable_type",
            F.to_timestamp("started_at").alias("started_at"),
            F.to_timestamp("ended_at").alias("ended_at"),
            "start_station_id", "start_station_name",
            "end_station_id",   "end_station_name",
            F.col("start_lat").cast("double").alias("start_lat"),
            F.col("start_lng").cast("double").alias("start_lng"),
            F.col("end_lat").cast("double").alias("end_lat"),
            F.col("end_lng").cast("double").alias("end_lng"),
            F.col("member_casual").alias("rider_type"),
            "_source_file", "_ingested_at",
        )
        .withColumn("duration_min",
            (F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")) / 60)
        .dropDuplicates(["ride_id"])
    )