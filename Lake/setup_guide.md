# Setup Guide — Uber Trip Analytics Data Lake (concise)

Local
- Create venv: python -m venv .venv && .venv\Scripts\activate
- Install: pip install -r requirements.txt
- Run local pipeline (use small sample first):
  python pyspark_jobs/ingestion.py --input data/nyc_uber.csv --output data/bronze --format csv
  python pyspark_jobs/cleaning.py --input data/bronze --output data/silver --partition_columns year,month,day
  python pyspark_jobs/transformations.py --input data/silver --output data/gold

EMR (high level)
- Upload scripts and data to S3: aws s3 cp -r pyspark_jobs/ s3://<bucket>/code/
- Create EMR cluster with Spark (emr-6.x). Ensure EMR role has S3 read/write.
- Add Spark steps referencing s3://<bucket>/code/*.py with proper args.
- Monitor via EMR console and CloudWatch.

Glue / Catalog
- Create Glue Crawlers targeting s3://<bucket>/bronze, /silver, /gold.
- Configure crawler IAM role and run to populate Glue Data Catalog.
- Use partition discovery for Silver/Gold Parquet outputs.

Athena
- Create external tables pointing to curated Gold prefixes (Parquet).
- Use queries in sql/athena_queries.sql.

Security & Performance notes
- Use s3a:// when accessing S3 from local Spark and include Hadoop AWS packages if needed.
- Partition by year/month/day to enable partition pruning.
- Use Parquet + snappy compression for cost and speed improvements.
