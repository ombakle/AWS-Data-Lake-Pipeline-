"""
Transformations job:
- Reads Silver partitioned parquet
- Produces analytics-ready tables:
  - daily_summary, monthly_revenue, peak_hour, driver_performance, location_popularity
- Writes each as parquet under <output>/<table_name>
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, sum as _sum, avg, round as _round
import argparse
import sys

def build_spark(app_name="uber_transform"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def main(args):
    spark = build_spark()
    df = spark.read.parquet(args.input)

    # Trip duration in minutes
    df = df.withColumn("trip_duration_minutes", (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long"))/60)

    out = args.output.rstrip("/")

    # Daily summary
    daily = df.groupBy("year","month","day").agg(
        count("trip_id").alias("total_trips"),
        _round(avg("trip_duration_minutes"),2).alias("avg_trip_minutes"),
        _round(avg("fare_amount"),2).alias("avg_fare")
    )
    daily.write.mode("overwrite").parquet(f"{out}/daily_summary")

    # Monthly revenue
    monthly = df.groupBy("year","month").agg(
        _round(_sum("fare_amount"),2).alias("total_revenue"),
        _round(avg("fare_amount"),2).alias("avg_fare")
    )
    monthly.write.mode("overwrite").parquet(f"{out}/monthly_revenue")

    # Peak hour
    peak = df.withColumn("hour", hour(col("pickup_datetime"))).groupBy("hour").agg(count("trip_id").alias("trips")).orderBy(col("trips").desc())
    peak.write.mode("overwrite").parquet(f"{out}/peak_hour")

    # Driver performance
    driver = df.groupBy("driver_id").agg(count("trip_id").alias("trips"), _round(avg("fare_amount"),2).alias("avg_fare")).orderBy(col("trips").desc())
    driver.write.mode("overwrite").parquet(f"{out}/driver_performance")

    # Location popularity (pickup)
    loc = df.groupBy("pickup_latitude","pickup_longitude").agg(count("trip_id").alias("pickup_count")).orderBy(col("pickup_count").desc())
    loc.write.mode("overwrite").parquet(f"{out}/location_popularity")

    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    parsed = p.parse_args()
    try:
        main(parsed)
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
