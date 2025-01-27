import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from Glue Data Catalog
customer_df = glueContext.create_dynamic_frame.from_catalog(database = "fraud_detection_project", table_name = "src_customer")
blacklist_df = glueContext.create_dynamic_frame.from_catalog(database = "fraud_detection_project",table_name = "src_blacklist")
transactions_df = glueContext.create_dynamic_frame.from_catalog(database = "fraud_detection_project", table_name = "src_transaction")

customer_spark_df = customer_df.toDF()
blacklist_spark_df = blacklist_df.toDF()
transactions_spark_df = transactions_df.toDF()
#rename customer_id in blacklist & transactions
blacklist_spark_df  = blacklist_spark_df.withColumnRenamed("customer_id","blacklist_customer_id")
transactions_spark_df = transactions_spark_df.withColumnRenamed("customer_id","transactions_customer_id")
#Data Filtering and Cleansing
customer_spark_df = customer_spark_df[customer_spark_df["customer_id"].isNotNull()]
customer_spark_df = customer_spark_df[customer_spark_df["transaction_limit"].isNotNull()]
transactions_spark_df = transactions_spark_df[transactions_spark_df["transactions_customer_id"].isNotNull()]
transactions_spark_df = transactions_spark_df[transactions_spark_df["amount"].isNotNull()]
transactions_spark_df = transactions_spark_df[transactions_spark_df["location"].isNotNull()]
# Convert transaction_time to timestamp
transactions_spark_df = transactions_spark_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
#Joining datasets
blacklist_cust = customer_spark_df.join(blacklist_spark_df, customer_spark_df.customer_id == blacklist_spark_df.blacklist_customer_id,"left")
blacklist_cust_inner = customer_spark_df.join(blacklist_spark_df, customer_spark_df.customer_id == blacklist_spark_df.blacklist_customer_id,"inner")
blacklist_cust_selected = blacklist_cust_inner.select("customer_id","name","email","reason","account_balance")
#blacklist_cust_selected.show()
#fradulent customers granular details
blacklist_tran_cust = blacklist_cust.join(transactions_spark_df, blacklist_cust.customer_id == transactions_spark_df.transactions_customer_id, "inner")
blacklist_tran_cust_selected = blacklist_tran_cust.select("customer_id","name","email","reason","account_balance","age_group","country","merchant_category","transaction_currency","merchant_id","transaction_channel","device_id")
#Handling Missing or Null Data
account_balance_mean = blacklist_tran_cust.select(avg("account_balance")).collect()[0][0]
transaction_amount_mean = blacklist_tran_cust.select(avg("amount")).collect()[0][0]
blacklist_tran_cust.withColumn("customer_status", 
                               when(col("customer_status").isNull(),"Inactive")
                               .otherwise(col("customer_status"))
                               )
blacklist_tran_cust.withColumn("account_balance"
                               ,when((col("account_balance").isNull()) | (col("account_balance") == 0), account_balance_mean)
                               .otherwise(col("account_balance"))
                               )
blacklist_tran_cust.withColumn("amount",
                                when((col("amount").isNull()) | (col("amount") == 0), transaction_amount_mean)
                               .otherwise(col("amount"))
                               )
#Data Enrichment Using Derived Columns
blacklist_tran_cust = blacklist_tran_cust.withColumn("risk_category", 
                                                      when(col("risk_score")<= 5, "Low Risk")
                                                     .when((col("risk_score")> 5) & (col("risk_score")<=7.5), "Medium Risk")
                                                     .otherwise("High Risk")
                                                     )
#all transactions in standard USD
blacklist_tran_cust = blacklist_tran_cust.withColumn('usd_amt',
                                                      when(col("preferred_currency") == "AUD",col("account_balance")*0.628)
                                                     .when(col("preferred_currency") == "GBP",col("account_balance")*1.236)
                                                     .when(col("preferred_currency") == "EUR",col("account_balance")*1.042)
                                                     .when(col("preferred_currency") == "JPY",col("account_balance")*0.006)
                                                     .otherwise(col("account_balance"))
                                                     )
#Outlier Detection and Removal
blacklist_tran_cust = blacklist_tran_cust.withColumn("need_check",
                                                     when((col("amount") > 1.5*transaction_amount_mean) | (col("amount") >= 2*account_balance_mean),"Y")
                                                     .otherwise("N")
                                                     )
#Fraud Detection and Risk Flagging
##Flagging Fraudulent Transactions
blacklist_tran_cust = blacklist_tran_cust.withColumn("fraud_check",
                                                     when((col("amount") > 5000),"High Risk")
                                                     .otherwise("Low Risk")
                                                     )
##Transaction Frequency Analysis
# Create a window partitioned by customer_id and ordered by transaction_time
window_spec = Window.partitionBy("transactions_customer_id").orderBy("timestamp")

# Calculate time difference between consecutive transactions
blacklist_tran_cust = blacklist_tran_cust.withColumn(
    "prev_transaction_time",
    lag("timestamp").over(window_spec)
).withColumn(
    "time_diff_seconds",
    (unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_transaction_time")))
)
# Flag transactions occurring within 60 seconds of the previous transaction
blacklist_tran_cust = blacklist_tran_cust.withColumn(
    "high_frequency_flag",
    when(col("time_diff_seconds") <= 60, 1).otherwise(0)
)
# Aggregate to count high-frequency transactions per customer
high_frequency_summary = blacklist_tran_cust.groupBy("customer_id").agg(
    count(when(col("high_frequency_flag") == 1, 1)).alias("high_frequency_count")
)
# Filter customers with high-frequency activity
threshold = 5
suspicious_customers = high_frequency_summary.filter(col("high_frequency_count") > threshold)
#suspicious_customers.show()
#Aggregating and Summarizing Data

black_tran_cust_agg = blacklist_tran_cust.groupby("customer_id").agg(
    sum(col("amount")).alias("total_transaction_amount"),
    avg(col("amount")).alias("average_transaction_amount"),
    count(col("amount")).alias("number_of_tran")
)
blacklist_cust_selected_dynamic_frame = DynamicFrame.fromDF(blacklist_cust_selected, glueContext, "blacklist_cust_selected_dynamic_frame")
black_tran_cust_agg_dynamic_frame = DynamicFrame.fromDF(black_tran_cust_agg, glueContext, "black_tran_cust_agg_dynamic_frame")
blacklist_tran_cust_dynamic_frame = DynamicFrame.fromDF(blacklist_tran_cust, glueContext, "blacklist_tran_cust_dynamic_frame")
# save to S3
glueContext.write_dynamic_frame.from_options(
    frame=blacklist_cust_selected_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://fraud-detection-project-bucket-01/glue_target/blacklisted_customer_details"},
    format="parquet"
)
glueContext.write_dynamic_frame.from_options(
    frame=black_tran_cust_agg_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://fraud-detection-project-bucket-01/glue_target/customer_aggr_details"},
    format="parquet"
)
glueContext.write_dynamic_frame.from_options(
    frame=blacklist_tran_cust_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://fraud-detection-project-bucket-01/glue_target/blacklisted_customer_consolidated_details"},
    format="parquet"
)
job.commit()