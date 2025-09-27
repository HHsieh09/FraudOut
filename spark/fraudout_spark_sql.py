from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Fraudout_detect") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalogImplementation","hive") \
    .enableHiveSupport() \
    .config("spark.sql.shuffle.partitions","4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql("use fraudout")
df = spark.sql("""select s.event_key,
            s.token,
            s.amount,
            s.truth_fraud,
            s.truth_score_prob,
            s.predicted_fraud,
            s.decision,
            c.user_transaction_time,
            c.country,
            c.mcc,
            c.device_id,
            c.synthetic_card_id,
            c.card_token,
            s.dt
        FROM tx_scores s
        JOIN tx_curated c
          ON s.token = c.token AND s.dt = c.dt""")
df.createOrReplaceTempView("main_score")

# 1. fraud rate per day
fraud_rate = spark.sql("""
    -- look at how many transactions are labeled fraud each day
    select dt,
           count(*) as total_tx,
           sum(truth_fraud) as fraud_tx,
           round(sum(truth_fraud) / count(*), 4) as fraud_rate
      from main_score
     group by dt
     order by dt
""")
fraud_rate.show()

# 2. confusion matrix style counts
confusion_matrix = spark.sql("""select
          sum(case when truth_fraud = 1 and predicted_fraud = 1 then 1 else 0 end) as true_positive,
          sum(case when truth_fraud = 0 and predicted_fraud = 1 then 1 else 0 end) as false_positive,
          sum(case when truth_fraud = 1 and predicted_fraud = 0 then 1 else 0 end) as false_negative,
          sum(case when truth_fraud = 0 and predicted_fraud = 0 then 1 else 0 end) as true_negative
          from main_score
""")
confusion_matrix.show()

confusion_matrix.createOrReplaceTempView("confusion_matrix")

metrics = spark.sql(""" select
        -- precision: out of all predicted fraud, how many were actually fraud
        case when true_positive + false_positive > 0 then round(true_positive / (true_positive + false_positive), 4) else null end as precision,
        -- recall: out of all actual fraud, how many we caught
        case when true_positive + false_negative > 0 then round(true_positive / (true_positive + false_negative), 4) else null end as recall,
        -- accuracy: overall hit rate
        case when true_positive + true_negative + false_positive + false_negative > 0 then round((true_positive + true_negative) / (true_positive + true_negative + false_positive + false_negative), 4) else null end as accuracy,
        -- f1: balance between precision and recall
        case when (2 * true_positive) + false_positive + false_negative > 0 then round((2 * true_positive) / ((2 * true_positive) + false_positive + false_negative), 4) else null end as f1,
        -- specificity: out of all actual non-fraud, how many we kept clean
        case when true_negative + false_positive > 0 then round(true_negative / (true_negative + false_positive), 4) else null end as specificity
    from confusion_matrix
""")

metrics.show()

# 3. fraud rate by mcc (merchant category code)
fraud_by_mcc = spark.sql("""
    -- check which merchant categories are riskier
    select mcc,
           count(*) as total_tx,
           sum(truth_fraud) as fraud_tx,
           round(sum(truth_fraud) / count(*), 3) as fraud_rate
      from main_score
     group by mcc
     order by fraud_rate desc
     limit 10
""")
fraud_by_mcc.show()

# 4. fraud probability by amount bucket
fraud_by_amount = spark.sql("""
    -- see how fraud rate changes with transaction size
    select case 
               when amount < 1000 then '< 1k'
               when amount between 1000 and 5000 then '1k - 5k'
               else '> 5k'
           end as amount_bucket,
           count(*) as total_tx,
           sum(truth_fraud) as fraud_tx,
           round(sum(truth_fraud) / count(*), 3) as fraud_rate
      from main_score
     group by case 
                  when amount < 1000 then '< 1k'
                  when amount between 1000 and 5000 then '1k - 5k'
                  else '> 5k'
              end
     order by amount_bucket
""")
fraud_by_amount.show()

# 5. fraud rate by country
fraud_by_country = spark.sql("""
    -- top countries by fraud rate
    select country,
           count(*) as total_tx,
           sum(truth_fraud) as fraud_tx,
           round(sum(truth_fraud) / count(*), 3) as fraud_rate
      from main_score
     group by country
     order by fraud_rate desc
     limit 10
""")
fraud_by_country.show()