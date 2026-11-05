from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment="Bronze: raw taxi trips as ingested")
def pl_bronze_trips():
    return (
        spark.read.table("samples.nyctaxi.trips")
        .withColumn("ingested_at", F.current_timestamp())
    )


@dp.materialized_view(comment="Silver: cleaned and validated trips")
@dp.expect_or_drop("positive_distance", "trip_distance > 0")
@dp.expect_or_drop("positive_fare", "fare_amount > 0")
@dp.expect_or_drop("dropoff_after_pickup", "tpep_dropoff_datetime > tpep_pickup_datetime")
def pl_silver_trips():
    return (
        spark.read.table("pl_bronze_trips")
        .withColumn(
            "trip_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 60,
        )
    )


@dp.materialized_view(comment="Gold: daily trips and average fare")
def pl_daily_summary():
    return (
        spark.read.table("pl_silver_trips")
        .groupBy(F.to_date("tpep_pickup_datetime").alias("pickup_date"))
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        )
    )