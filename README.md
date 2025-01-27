#AWS Fraud Detection Project

##Problem Statement 

Fraudulent activities such as money laundering, account takeovers, and suspicious spending pose significant challenges to financial institutions. Detecting these activities in real-time is critical for preventing financial loss and ensuring secure transactions. Existing fraud detection systems need effective and scalable methods to analyze customer and transaction data.

##Solution 

This project implements a fraud detection pipeline on AWS, designed to identify and mitigate fraudulent activities. Using AWS Glue, AWS Lambda, and Amazon S3, the pipeline extracts data from an RDS MySQL database, applies various transformations, and loads the cleaned data to S3 for further analysis.

The solution includes:

Data cleansing and enrichment (e.g., currency conversion, risk categorization).
Outlier detection and high-frequency transaction analysis to flag abnormal or suspicious activity.
Blacklist management to monitor high-risk customers and their transaction patterns.
Risk scoring and categorization of transactions based on predefined thresholds.
By analyzing transaction patterns and customer data, this pipeline helps to identify fraudulent transactions and reduces the risk of financial loss. The data is stored in Parquet format for efficient querying and analysis.