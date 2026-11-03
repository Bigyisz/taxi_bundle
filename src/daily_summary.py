# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_catalog")
dbutils.widgets.text("schema", "sara_bartha")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

trips = spark.read.table(f"{catalog}.{schema}.clean_trips")

summary = (
    trips
    .groupBy(F.to_date("tpep_pickup_datetime").alias("pickup_date"))
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    )
    .orderBy("pickup_date")
)

summary.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.daily_summary")