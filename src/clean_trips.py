# Databricks notebook source
# Silver layer: clean the bronze trips using the unit-tested transform.
from taxi_transforms import clean_trips

dbutils.widgets.text("catalog", "test_catalog")
dbutils.widgets.text("schema", "sara_bartha")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

bronze = spark.read.table(f"{catalog}.{schema}.bronze_trips")
clean = clean_trips(bronze)

(
    clean.write.mode("overwrite")
    .option("overwriteSchema", "true")  # full refresh: let the table follow the new columns
    .saveAsTable(f"{catalog}.{schema}.clean_trips")
)
