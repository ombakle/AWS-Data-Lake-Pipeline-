# How to run this project

## Project Summary
A compact, production-style Data Lake that processes NYC Uber trip data using a Medallion architecture (Bronze → Silver → Gold). The pipeline ingests raw CSVs into S3/local Bronze, performs PySpark cleaning and validation to produce partitioned Parquet in Silver, and generates analytics-ready Gold tables for KPIs (daily trips, peak hours, revenue trends, driver performance, location popularity). Designed for easy demo, local development, and deployment on AWS (EMR/Glue/Athena).

## Project Description
This project demonstrates end-to-end data engineering practices:
- Ingestion: flexible readers for CSV/JSON/Parquet, schema validation, and ingestion timestamps to preserve lineage.
- Cleansing: deduplication, null handling, timestamp normalization, type casting, and partitioned Parquet output for efficient querying.
- Transformations: business-focused aggregations and windowed computations to produce daily summaries, monthly revenue, peak-hour analysis, driver metrics, and location popularity tables.
- Deployment-ready: scripts runnable locally via python or spark-submit, and deployable to AWS EMR or Glue. Glue Crawlers populate the Glue Data Catalog and Athena provides interactive SQL analytics on Gold tables.
- Quality & operations: includes lightweight pytest checks, recommendations for CloudWatch logging, partitioning and Parquet/Compression for performance, and guidance for IAM/secure data access.

## Summary
This project implements an end-to-end "Uber Trip Analytics" Data Lake using a Medallion (Bronze → Silver → Gold) architecture. It ingests NYC Uber trip CSVs into a Bronze (raw) layer, performs PySpark cleaning and validation to produce a Silver (clean, partitioned Parquet) layer, and generates Gold (analytics-ready) Parquet tables with KPIs such as daily trips, peak hours, revenue trends, driver performance, and location popularity. Designed for demo/interview use and easily runnable locally or on AWS (EMR/Glue + Athena).

## Requirements
- Local dev
  - Python 3.8+
  - Java 8+ (required by Spark)
  - Apache Spark 3.x (for spark-submit local runs) or use pyspark package for lightweight tests
  - Git (optional, for pushing repo)
- Python packages (install via pip install -r requirements.txt)
  - pyspark
  - boto3
  - pyarrow
  - pandas
  - pytest (for tests)
- AWS (for cloud runs)
  - AWS account with S3, EMR, Glue, Athena permissions
  - IAM roles for EMR/Glue with least-privilege S3 access
  - AWS CLI configured (aws configure) or environment credentials
- Data & storage
  - NYC Uber trip CSV (sample or full dataset); store raw in s3://<bucket>/raw/ or data/raw locally
  - Recommend using Parquet + Snappy for Silver/Gold to optimize query performance and cost
- Recommended resources for medium workload
  - Local dev: 8+ CPU cores, 16+ GB RAM
  - EMR: m5.xlarge or r5.xlarge workers (3+ nodes) for tens of millions of rows
- Notes
  - Use s3a:// when accessing S3 from Spark; ensure Hadoop AWS jars available if running spark-submit locally with S3.
  - Replace placeholder bucket names, paths, and instance types with environment-appropriate values.

1) Prepare environment (Windows)
- Create and activate virtual env:
  ```
  python -m venv .venv
  .venv\Scripts\activate
  ```

- Install dependencies:
  ```
  pip install -r requirements.txt
  ```

2) Run locally (Python runner)
- Ingest CSV into local bronze folder:
  ```
  python pyspark_jobs/ingestion.py --input data/nyc_uber.csv --output data/bronze --format csv
  ```

- Clean (bronze -> silver):
  ```
  python pyspark_jobs/cleaning.py --input data/bronze --output data/silver --partition_columns year,month,day
  ```

- Transform (silver -> gold):
  ```
  python pyspark_jobs/transformations.py --input data/silver --output data/gold
  ```

3) Run with spark-submit (if Spark installed)
- Local files:
  ```
  spark-submit --master local[4] pyspark_jobs/ingestion.py --input data/nyc_uber.csv --output data/bronze --format csv
  ```

- Direct S3 access from local Spark (requires Hadoop AWS libs and credentials):
  ```
  spark-submit --master local[4] \
    --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.118 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    pyspark_jobs/ingestion.py --input s3a://your-bucket/raw/nyc_uber.csv --output s3a://your-bucket/bronze --format csv
  ```

4) Run on AWS EMR (high-level)
- Upload code/data:
  ```
  aws s3 cp -r pyspark_jobs/ s3://<bucket>/code/
  aws s3 cp data/nyc_uber.csv s3://<bucket>/raw/
  ```

- Example create-cluster + step (simplified):
  ```
  aws emr create-cluster --release-label emr-6.6.0 --applications Name=Spark \
    --ec2-attributes KeyName=<key>,InstanceProfile=EMR_EC2_DefaultRole \
    --instance-type m5.xlarge --instance-count 3 \
    --steps Type=Spark,Name="Ingest",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://<bucket>/code/ingestion.py,--input,s3://<bucket>/raw/nyc_uber.csv,--output,s3://<bucket>/bronze]
  ```

5) Run on AWS Glue
- Upload script to S3 and create a Glue PySpark job pointing to s3://<bucket>/code/ingestion.py
- Configure IAM role, worker type, and script arguments (--input, --output)
- Run Glue Crawlers on s3://<bucket>/bronze, /silver, /gold to populate Glue Catalog

6) Query with Athena
- Ensure Gold is Parquet and partitioned (year/month/day)
- Create external table in Athena via DDL pointing to s3://<bucket>/gold/<table>/
- Run queries from sql/athena_queries.sql or Athena console

Troubleshooting / tips
- Use s3a:// when Spark accesses S3; ensure AWS credentials available to JVM (env vars or ~/.aws).
- For local S3 access add Hadoop AWS packages in spark-submit (--packages).
- Test pipeline on a small sample locally before scaling to EMR/Glue.