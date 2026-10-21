from pyspark import pipelines as dp
from pyspark.sql import functions as F

def valid_trips():
    """Silver trips filtered for analysis: real durations only."""
    return (spark.read.table("silver_trips")
            .filter(F.col("duration_min").between(1, 1440)))


@dp.materialized_view(
    name="gold_daily_station_rides",
    comment="Rides per start station per day, with member share and avg duration"
)
def gold_daily_station_rides():
    return (valid_trips()
        .filter(F.col("start_station_id").isNotNull())
        .groupBy(F.to_date("started_at").alias("ride_date"), "start_station_id")
        .agg(
            F.max("start_station_name").alias("start_station_name"),
            F.count("*").alias("rides"),
            F.sum(F.when(F.col("rider_type") == "member", 1).otherwise(0)).alias("member_rides"),
            F.round(F.avg("duration_min"), 1).alias("avg_duration_min"),
        ))


@dp.materialized_view(
    name="gold_hourly_demand",
    comment="Ride counts by day of week, hour, and rider type"
)
def gold_hourly_demand():
    return (valid_trips()
        .groupBy(
            F.dayofweek("started_at").alias("day_num"),
            F.date_format("started_at", "EEEE").alias("day_of_week"),
            F.hour("started_at").alias("hour"),
            "rider_type",
        )
        .agg(F.count("*").alias("rides")))


@dp.materialized_view(
    name="gold_rider_summary",
    comment="Ride volume and duration by rider type and bike type"
)
def gold_rider_summary():
    return (valid_trips()
        .groupBy("rider_type", "rideable_type")
        .agg(
            F.count("*").alias("rides"),
            F.round(F.avg("duration_min"), 1).alias("avg_duration_min"),
            F.round(F.percentile_approx("duration_min", 0.5), 1).alias("median_duration_min"),
        ))