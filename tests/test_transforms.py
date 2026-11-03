import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).parent.parent / "src"))
from taxi_transforms import clean_trips


def test_clean_trips_removes_bad_rows():
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    start = datetime(2026, 1, 1, 10, 0)
    end = datetime(2026, 1, 1, 10, 30)
    rows = [
        (1.5, 10.0, start, end),   # good trip
        (0.0, 5.0, start, end),    # zero distance
        (2.0, -1.0, start, end),   # negative fare
    ]
    cols = ["trip_distance", "fare_amount",
            "tpep_pickup_datetime", "tpep_dropoff_datetime"]
    result = clean_trips(spark.createDataFrame(rows, cols)).collect()

    assert len(result) == 1
    assert result[0]["trip_minutes"] == 30