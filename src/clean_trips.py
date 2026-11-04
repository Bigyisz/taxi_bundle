# Databricks notebook source
from pyspark.sql import functions as F
from taxi_transforms import clean_trips

clean = clean_trips(spark.read.table("samples.nyctaxi.trips"))
dbutils.widgets.text("catalog", "test_catalog")
dbutils.widgets.text("schema", "sara_bartha")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
target_table = f"{catalog}.{schema}.clean_trips"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

trips = spark.read.table(f"{catalog}.{schema}.bronze_trips")

clean = (
    trips
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("fare_amount") > 0)
    .withColumn(
        "trip_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime")
         - F.unix_timestamp("tpep_pickup_datetime")) / 60,
    )
)

clean.write.mode("overwrite").saveAsTable(target_table)