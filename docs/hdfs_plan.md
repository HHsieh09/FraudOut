### For raw json data
user/spark/fraudout/raw/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.json

### For Spark-cleaned data
user/spark/fraudout/curated/tx_curated/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.parquet

### For feature table such as velocity, geographical distance, and holiday flags
user/spark/fraudout/curated/tx_features/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.parquet

### For scoring results from streaming rules or models, including score and decision
user/spark/fraudout/curated/tx_scores/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.parquet

### For Deequ's data quality check
user/spark/fraudout/curated/dq_results/yyyy=YYYY/mm=MM/dd=DD/hour=HH/*.parquet

### Usage
`hdfs df -mkdir -p user/spark/fraudout/raw`
`hdfs df -mkdir -p user/spark/fraudout/curated/tx_curated`
`hdfs df -mkdir -p user/spark/fraudout/curated/tx_features`
`hdfs df -mkdir -p user/spark/fraudout/curated/tx_scores`
`hdfs df -mkdir -p user/spark/fraudout/curated/dq_results`