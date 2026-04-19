import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- 1. Initialize Glue + Spark context ---
args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_bucket", "dest_bucket"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = f"s3://{args['source_bucket']}/raw/users/"
DEST_PATH   = f"s3://{args['dest_bucket']}/processed/users/"

logger.info(f"Reading from: {SOURCE_PATH}")
logger.info(f"Writing to:   {DEST_PATH}")

# --- 2. Extract — read raw CSV from S3 ---
raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .csv(SOURCE_PATH)

logger.info(f"Rows ingested: {raw_df.count()}")
raw_df.printSchema()

# --- 3. Transform — clean and enrich ---

# Drop rows where critical fields are null
df = raw_df.dropna(subset=["user_id", "email"])

# Deduplicate by user_id, keep most recent record
df = df.dropDuplicates(["user_id"])

# Normalize: lowercase email, strip whitespace
df = df.withColumn("email", F.lower(F.trim(F.col("email"))))

# Cast types explicitly (inferSchema can guess wrong)
df = df.withColumn("age",     F.col("age").cast(IntegerType())) \
       .withColumn("revenue", F.col("revenue").cast(DoubleType()))

# Filter out obviously bad data
df = df.filter(F.col("age").between(0, 120))
df = df.filter(F.col("revenue") >= 0)

# Enrich: add a processing timestamp
df = df.withColumn("processed_at", F.current_timestamp())

# Enrich: derive a simple age bucket column
df = df.withColumn("age_group", F.when(F.col("age") < 25, "18-24")
                                 .when(F.col("age") < 35, "25-34")
                                 .when(F.col("age") < 50, "35-49")
                                 .otherwise("50+"))

logger.info(f"Rows after cleaning: {df.count()}")

# --- 4. Load — write clean data to S3 as Parquet ---
df.write \
    .mode("overwrite") \
    .partitionBy("age_group") \
    .parquet(DEST_PATH)

logger.info("ETL job complete. Data written successfully.")

# Write clean data partitioned by age_group for faster downstream queries
df.write \
    .mode("overwrite") \
    .partitionBy("age_group") \
    .parquet(DEST_PATH)

job.commit()
