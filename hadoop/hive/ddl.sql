# External table for raw JSON data, partitioned by date and hour
CREATE EXTERNAL TABLE IF NOT EXISTS raw_data (
    event_id string,
    event_timestamp string,
    amount decimal(20,2),
    currency string,
    response_code string,
    network string,
    merchant struct<mid:string,mcc:string>,
    country string,
    channel string,
    device_id string,
    lat double,
    lon double
)
PARTITIONED BY (yyyy int, mm int, dd int, hour int)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/data/fraudout/raw';

# External table for curated Parquet data, partitioned by date and hour
CREATE EXTERNAL TABLE IF NOT EXISTS tx_curated (
    event_id string,
    event_timestamp timestamp,
    amount decimal(20,2),
    currency string,
    mcc string,
    country string,
    channel string,
    device_id string
)
PARTITIONED BY (yyyy int, mm int, dd int, hour int)
STORED AS PARQUET
LOCATION '/data/fraudout/curated/tx_curated';