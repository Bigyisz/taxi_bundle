from pyspark.sql import DataFrame, functions as F


def clean_trips(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .withColumn(
            "trip_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 60,
        )
    )