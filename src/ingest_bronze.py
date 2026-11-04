# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_catalog")
dbutils.widgets.text("schema", "sara_bartha")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

raw = spark.read.table("samples.nyctaxi.trips")

bronze = raw.withColumn("ingested_at", F.current_timestamp())

bronze.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_trips")