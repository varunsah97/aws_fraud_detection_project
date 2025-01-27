import json
import pymysql
import pandas as pd
import boto3
import io


#Recommened to use AWS KMS
host = "********"
user = "********"
password = "*******"
database = "fraud_detection_project"

# Query to fetch data
query = "SELECT * FROM src_blacklist"

def lambda_handler(event, context):
    con = pymysql.connect(host=host, user=user, password=password, database=database)
    s3_bucket = "fraud-detection-project-bucket-01"
    s3_key = "source/blacklist/blacklist.csv"
    
    with con.cursor() as cur:
        cur.execute(query)
        result = cur.fetchall()
        blacklist_src = pd.DataFrame(result, columns=['blacklist_id','customer_id','reason','date_flagged','blacklist_type','notes','expiry_date','last_reviewed_date','blacklist_origin'])
    
    # Close the connection
    con.close()

    blacklist_src['date_flagged'] = pd.to_datetime(blacklist_src['date_flagged'])
    blacklist_src['expiry_date'] = pd.to_datetime(blacklist_src['expiry_date'])
    blacklist_src['last_reviewed_date'] = pd.to_datetime(blacklist_src['last_reviewed_date'])
    blacklist_src['blacklist_type'] = blacklist_src['blacklist_type'].str.lower().str.strip()
    blacklist_src['blacklist_origin'] = blacklist_src['blacklist_origin'].str.replace(" ", "_").str.strip()
    blacklist_src["reason"] = blacklist_src["reason"].fillna("Unknown")
    blacklist_src = blacklist_src[blacklist_src["customer_id"].notnull() & blacklist_src["date_flagged"].notnull()]
    blacklist_final = blacklist_src.drop_duplicates()

    # Convert DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    blacklist_final.to_csv(csv_buffer, index=False, header=True)

    # Upload the CSV file to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

    #start blacklist crawler
    glue_client = boto3.client('glue')
    crawler_name = 'blacklist_crawl'
    glue_client.start_crawler(Name=crawler_name)
    

    return {
        'statusCode': 200,
        'body': json.dumps('Data Successfully Loaded to S3 and crawler started')
    }