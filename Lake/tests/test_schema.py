import csv
from pathlib import Path

EXPECTED = {"trip_id","pickup_datetime","dropoff_datetime","fare_amount","driver_id"}

def test_sample_csv_header(tmp_path):
    sample = tmp_path / "sample.csv"
    sample.write_text("trip_id,pickup_datetime,dropoff_datetime,fare_amount,driver_id\n")
    with sample.open() as f:
        header = next(csv.reader(f))
    assert EXPECTED.issubset(set(header))
