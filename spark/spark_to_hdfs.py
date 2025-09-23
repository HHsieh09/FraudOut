from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, get_json_object, col, date_format
from pyspark.sql.types import StructType, StructField, StringType
import os

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092" 
KAFKA_TOPICS = "fraudout.txn.raw"

OUTPUT_PATH = "hdfs://namenode:8020/user/spark/fraudout/bronze"
CHECKPOINT_DIR = "hdfs://namenode:8020/user/spark/fraudout/checkpoints"

# Create Spark session
# config("spark.sql.shuffle.partitions","4") sets the number of shuffle partitions to 4
spark = SparkSession.builder \
    .appName("fraudout_kafka_to_hdfs") \
    .config("spark.sql.shuffle.partitions","4") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka topic
# option("kafka.bootstrap.servers") specifies the Kafka server address
# option("subscribe") specifies the Kafka topic to subscribe to
# option("startingOffsets") specifies where to start reading messages (latest or earliest)
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPICS) \
    .option("stratingPffsets", "latest") \
    .load()

# Select the value from the Kafka message and cast it to string
# The value is the actual message content
kafka_df = df.selectExpr("CAST(key as STRING) as event_key",
                        "CAST(value as STRING) as event_json",
                        "timestamp as kafka_ts") \
            .withColumn("ingestion_ts", current_timestamp()) 
            # kafka timestamp means the time when the message was produced to Kafka
            # ingestion_ts means the time when the message was ingested into Spark
            # They are used to measure the end-to-end latency

# Define the schema for the incoming JSON data
# This schema matches the structure of the JSON messages in the Kafka topic
txn_schema = StructType([
    StructField("type", StringType()),
    StructField("country", StringType()),
    StructField("merchant", StringType()),
    StructField("token", StringType()),
    StructField("amount", StringType()),
    StructField("network", StringType()),
    StructField("channel", StringType()),
    StructField("created_time", StringType()),
    StructField("user_transaction_time", StringType()),
    StructField("currency_code", StringType()),
])

# Extract fields from the JSON data using the defined schema
# get_json_object is used to extract specific fields from the JSON string
# date_format is used to create a date column for partitioning the output data
txn_df = kafka_df.select(
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

# Await termination of the streaming query
query.awaitTermination()