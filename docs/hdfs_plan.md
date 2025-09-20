### For raw json data
/data/fraudout/raw/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.json

### For Spark-cleaned data
/data/fraudout/curated/tx_curated/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.parquet

### For feature table such as velocity, geographical distance, and holiday flags
/data/fraudout/curated/tx_features/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.parquet

### For scoring results from streaming rules or models, including score and decision
/data/fraudout/curated/tx_scores/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.parquet

### For Deequ's data quality check
/data/fraudout/curated/dq_results/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.parquet

### Usage
`hdfs df -mkdir -p /data/fraudout/raw`
`hdfs df -mkdir -p /data/fraudout/curated/tx_curated`
`hdfs df -mkdir -p /data/fraudout/curated/tx_features`
`hdfs df -mkdir -p /data/fraudout/curated/tx_scores`
`hdfs df -mkdir -p /data/fraudout/curated/dq_results`