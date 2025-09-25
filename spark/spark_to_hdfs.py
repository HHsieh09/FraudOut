from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, get_json_object, col, date_format, from_json,  window, count, sum as _sum, lit, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

# Define Kafka parameters
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092" 
KAFKA_TOPICS = "fraudout.txn.raw"

# Define HDFS paths
RAW_PATH = "hdfs://namenode:8020/user/spark/fraudout/raw"
CURATED_PATH = "hdfs://namenode:8020/user/spark/fraudout/curated/tx_curated"
FEATURES_PATH = "hdfs://namenode:8020/user/spark/fraudout/curated/tx_features"
SCORES_PATH = "hdfs://namenode:8020/user/spark/fraudout/curated/tx_scores"
DQ_PATH = "hdfs://namenode:8020/user/spark/fraudout/curated/dq_results"

# Define checkpoint paths
CHECKPOINT_DIR = "hdfs://namenode:8020/user/spark/fraudout/checkpoints"

# Create Spark session
# config("spark.sql.shuffle.partitions","4") sets the number of shuffle partitions to 4
spark = SparkSession.builder \
    .appName("fraudout_kafka_to_hdfs") \
    .config("spark.sql.shuffle.partitions","4") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Define the schema for the incoming JSON data.
# Only define the top-level fields; nested fields are extracted using get_json_object
# This schema matches the structure of the JSON messages in the Kafka topic

txn_schema = StructType([
    StructField("type", StringType()),
    StructField("token", StringType()),
    StructField("amount", DoubleType()),          # 若原始 JSON 有時是字串，改成 StringType 再 cast
    StructField("network", StringType()),
    StructField("created_time", StringType()),
    StructField("user_transaction_time", StringType()),
    StructField("currency_code", StringType()),
])

# Read from Kafka topic
# option("kafka.bootstrap.servers") specifies the Kafka server address
# option("subscribe") specifies the Kafka topic to subscribe to
# option("startingOffsets") specifies where to start reading messages (latest or earliest)
source = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPICS) \
    .option("startingOffsets", "latest") \
    .load()

# Sink 1: raw json data to HDFS
# Select the value from the Kafka message and cast it to string
# The value is the actual message content
raw_df = source.selectExpr("CAST(key as STRING) as event_key",
                        "CAST(value as STRING) as event_json",
                        "timestamp as kafka_ts") \
            .withColumn("ingestion_ts", current_timestamp()) 
            # kafka timestamp means the time when the message was produced to Kafka
            # ingestion_ts means the time when the message was ingested into Spark
            # They are used to measure the end-to-end latency

raw_df = raw_df.withColumn("dt", date_format(col("ingestion_ts"), "yyyy-MM-dd"))

# Write the raw data to HDFS in JSON format
raw_query = raw_df.writeStream \
    .format("json") \
    .option("path", RAW_PATH) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw") \
    .partitionBy("dt") \
    .outputMode("append") \
    .start()

# Sink 2: curated parquet data to HDFS
# Parse the JSON data using the defined schema and flatten the structure
# withColumn("txn", from_json(...)) creates a new column "txn" by parsing the JSON string
# select("event_key","event_json","kafka_ts", "ingestion_ts", "txn.*") selects the original columns and flattens the "txn" struct into individual columns
curated_df = raw_df.withColumn("txn", from_json(col("event_json"), txn_schema)) \
    .select("event_key","event_json","kafka_ts", "ingestion_ts", "dt", "txn.*",
        get_json_object("event_json", "$.card_acceptor.name").alias("merchant"),
        get_json_object("event_json", "$.card_acceptor.country_code").alias("country"),
        get_json_object("event_json", "$.transaction_metadata.payment_channel").alias("channel"))

# Write the curated data to HDFS in Parquet format
curated_query = curated_df.writeStream \
    .format("parquet") \
    .option("path", CURATED_PATH) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/curated") \
    .partitionBy("dt") \
    .outputMode("append") \
    .start()

# Sink 3: feature engineered data to HDFS
features_base = curated_df\
    .withColumn("amount_double", col("amount").cast("double")) \
    .withColumn("event_ts", to_timestamp(col("user_transaction_time")))

# Perform feature engineering by aggregating transaction counts and amounts over a 5-minute window
# withColumn("amount_double", col("amount").cast(DoubleType())) casts the amount to DoubleType for aggregation
# withColumn("event_ts", col("user_transaction_time").cast(TimestampType())) casts the user_transaction_time to TimestampType for windowing
# withWatermark("event_ts","10 minutes") sets a watermark on event_ts to handle late data. Data older than 10 minutes will be ignored
# groupBy(col("token"), window(col("event_ts", "5 minutes"))) groups the data by token and 5-minute window
# agg(count("*").alias("txn_count_5min"), _sum("amount_double").alias("txn_amount_5min")) computes the count and sum of transactions in each group
# withColumn("dt", date_format(col("window.end"), "yyyy-MM-dd")) creates a date column for partitioning the output data. The date is based on the end of the window
features_df = features_base.withWatermark("event_ts","10 minutes") \
    .groupBy(
        col("token"),
        window(col("event_ts"), "5 minutes")
    ).agg(
        count("*").alias("txn_count_5min"),
        _sum("amount_double").alias("txn_amount_5min")
    ) \
    .withColumn("dt", date_format(col("window.end"), "yyyy-MM-dd"))

# Write the feature engineered data to HDFS in Parquet format
feature_query = features_df.writeStream \
    .format("parquet") \
    .option("path", FEATURES_PATH) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/features") \
    .partitionBy("dt") \
    .outputMode("append") \
    .start()

# Sink 4: scores data to HDFS
# Simple scoring logic: if amount > 1000, score = 1 (fraud), else score = 0 (not fraud)
# withColumn("score", ...) creates a new column "score" based on the amount
# withColumn("decision", ...) creates a new column "decision" based on the amount
scores_df = curated_df.withColumn("score", (col("amount").cast(DoubleType()) > 1000).cast("int")) \
    .withColumn("decision", when(col("amount").cast("double") > 1000, lit("REVIEW")).otherwise(lit("OK"))) \
    .withColumn("dt", date_format(col("ingestion_ts"), "yyyy-MM-dd"))

# Write the scores data to HDFS in Parquet format
scores_query = scores_df.writeStream \
    .format("parquet") \
    .option("path", SCORES_PATH) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/scores") \
    .partitionBy("dt") \
    .outputMode("append") \
    .start()

# Sink 5: data quality results to HDFS
dq_df = curated_df.select(
    col("token").isNotNull().alias("token_not_null"),
    (col("amount").cast(DoubleType()) > 0).alias("amount_positive"),
    current_timestamp().alias("dq_run_ts"),
    date_format(col("ingestion_ts"), "yyyy-MM-dd").alias("dt")
)

# Write the data quality results to HDFS in Parquet format
dq_query = dq_df.writeStream \
    .format("parquet") \
    .option("path", DQ_PATH) \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/dq") \
    .partitionBy("dt") \
    .outputMode("append") \
    .start()

"""
# Extract fields from the JSON data using the defined schema
# get_json_object is used to extract specific fields from the JSON string
# date_format is used to create a date column for partitioning the output data
txn_df = raw_df.select(
    "event_key",
    "event_json",
    "ingestion_ts",
    get_json_object("event_json", "$.transaction.type").alias("txn_type"),
    get_json_object("event_json", "$.transaction.country").alias("txn_country"),
    get_json_object("event_json", "$.transaction.merchant").alias("txn_merchant"),
    get_json_object("event_json", "$.transaction.token").alias("txn_token"),
    get_json_object("event_json", "$.transaction.amount").alias("txn_amount"),
    get_json_object("event_json", "$.transaction.network").alias("txn_network"),
    get_json_object("event_json", "$.transaction.channel").alias("txn_channel"),
    get_json_object("event_json", "$.transaction.created_time").alias("txn_created_time"),
    get_json_object("event_json", "$.transaction.user_transaction_time").alias("txn_user_transaction_time"),
    get_json_object("event_json", "$.transaction.currency_code").alias("txn_currency_code")
).withColumn("date", date_format(col("ingestion_ts"), "yyyy-MM-dd"))

# Write the processed data to HDFS in Parquet format
# option("path") specifies the output path in HDFS
# option("checkpointLocation") specifies the checkpoint directory for fault tolerance
# partitionBy("date") partitions the output data by date for efficient querying
# outputMode("append") appends new data to the existing data
# start() starts the streaming query
query = txn_df.writeStream \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .partitionBy("date") \
    .outputMode("append") \
    .start()
"""
# Await termination of all streams
spark.streams.awaitAnyTermination()