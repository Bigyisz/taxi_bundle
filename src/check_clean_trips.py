# Databricks notebook source
# Check the clean_trips table to ensure it has rows after the cleaning process.

dbutils.widgets.text("catalog", "test_catalog")
dbutils.widgets.text("schema", "sara_bartha")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

clean_count = spark.read.table(f"{catalog}.{schema}.clean_trips").count()
print(f"Number of rows in clean_trips: {clean_count}")

if clean_count == 0:
    raise ValueError("clean_trips table is empty. Please check the clean_trips function.")