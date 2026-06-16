"""
Simple ingestion job:
- Supports csv/json/parquet input
- Validates presence of required columns
- Adds ingestion_timestamp
- Writes parquet (append) to output (local or s3a)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import argparse
import sys

REQUIRED_COLS = {"trip_id", "pickup_datetime", "dropoff_datetime", "fare_amount", "driver_id"}

def build_spark(app_name="uber_ingestion"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def validate_schema(df):
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def read_input(spark, path, fmt):
    fmt = fmt.lower()
    if fmt == "csv":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    if fmt == "json":
        return spark.read.json(path)
    if fmt == "parquet":
        return spark.read.parquet(path)
    raise ValueError("Unsupported format: csv|json|parquet")

def main(args):
    spark = build_spark()
    df = read_input(spark, args.input, args.format)
    validate_schema(df)
    df_out = df.withColumn("ingestion_timestamp", current_timestamp())
    df_out.write.mode("append").parquet(args.output)
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--format", default="csv", help="csv|json|parquet")
    parsed = p.parse_args()
    try:
        main(parsed)
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
