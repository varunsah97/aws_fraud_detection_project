# AWS Fraud Detection Project
<!--Remove the below lines and add yours -->
A short description about the script must be mentioned here.

###Problem Statement 

Fraudulent activities such as money laundering, account takeovers, and suspicious spending pose significant challenges to financial institutions. Detecting these activities in real-time is critical for preventing financial loss and ensuring secure transactions. Existing fraud detection systems need effective and scalable methods to analyze customer and transaction data.

### Solution

The fraud detection pipeline is built using a combination of AWS services including AWS Glue, AWS Lambda, and Amazon S3, providing a scalable and efficient way to detect fraud in customer transactions.

Data Extraction and Transformation:

AWS Lambda is used to fetch blacklist data from an RDS MySQL database. The Lambda function extracts data from the src_blacklist table and performs basic transformations such as cleaning date fields, normalizing text, and handling missing values. The cleaned data is then saved as a CSV file in an S3 bucket. After uploading to S3, the Lambda function triggers an AWS Glue crawler to update the Glue catalog with the new data.
Data Processing in AWS Glue:

The data from multiple sources is loaded into AWS Glue as DynamicFrames, specifically from the Glue Data Catalog using from_catalog.
The data includes customer details, blacklist data, and transaction data. The customer dataset is joined with the blacklist dataset to filter out flagged customers. Transaction data is then integrated into this dataset to track fraudulent transactions.
The pipeline performs various data transformation operations like:
Data cleansing: Removing null values and filtering invalid records.
Data enrichment: Adding new derived columns such as risk categories and converting transaction amounts to a standard currency (USD).
Outlier detection: Identifying transactions that are significantly larger than average to flag them for further review.
Fraudulent Transaction Detection:

Fraudulent transactions are identified based on transaction amount thresholds. Transactions over a certain amount (e.g., $5000) are flagged as "High Risk."
The system also analyzes the frequency of transactions, flagging customers with an unusually high number of transactions within a short time frame (e.g., within 60 seconds). This identifies potential money laundering or account takeover attempts.
Risk Categorization:

Customers and transactions are classified into different risk categories (e.g., Low Risk, Medium Risk, and High Risk) based on a risk score derived from transaction characteristics. This helps prioritize further investigation.
Aggregating and Storing Data:

After the necessary transformations, the data is aggregated to provide insights into customer behaviors, such as total transaction amount, average transaction value, and the number of transactions.
The final processed data is written to Amazon S3 in Parquet format, which ensures both high performance and cost efficiency for large datasets.
Monitoring and Automation:

The system is fully automated, with Glue triggers orchestrating the data processing pipeline from data extraction to transformation and storage.
Glue crawlers update the Glue catalog with each new batch of data, enabling users to query the processed data efficiently for analytics or further processing.
By automating the extraction, transformation, and loading (ETL) of fraud detection data, this solution enhances the ability to monitor and flag suspicious activity, reducing manual intervention and improving the speed at which fraudulent transactions can be detected and mitigated.

### Screenshot/GIF showing the sample use of the script
<!--Remove the below lines and add yours -->




