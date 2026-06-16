"""
Cleaning job:
- Reads Bronze Parquet (or folder) then:
  - drops duplicates by trip_id
  - casts numeric fields
  - parses timestamps
  - filters rows missing critical fields
  - adds year/month/day partition columns
  - writes partitioned Parquet to Silver path
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth
import argparse
import sys

def build_spark(app_name="uber_cleaning"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def main(args):
    spark = build_spark()
    df = spark.read.parquet(args.input)

    # Deduplicate
    df = df.dropDuplicates(["trip_id"])

    # Parse timestamps and cast numeric columns
    df = df.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"))) \
           .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"))) \
           .withColumn("fare_amount", col("fare_amount").cast("double")) \
           .withColumn("trip_distance", col("trip_distance").cast("double"))

    # Filter out records missing critical fields
    df = df.filter(col("pickup_datetime").isNotNull() & col("dropoff_datetime").isNotNull() & col("driver_id").isNotNull())

    # Add partition columns
    df = df.withColumn("year", year(col("pickup_datetime"))) \
           .withColumn("month", month(col("pickup_datetime"))) \
           .withColumn("day", dayofmonth(col("pickup_datetime")))

    # Write partitioned parquet; default partition columns if none provided
    partition_cols = args.partition_columns.split(",") if args.partition_columns else ["year","month","day"]
    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(args.output)
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--partition_columns", default="year,month,day", help="comma-separated")
    parsed = p.parse_args()
    try:
        main(parsed)
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
