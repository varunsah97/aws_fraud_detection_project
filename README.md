# AWS Fraud Detection Project

### Problem Statement 

Fraudulent activities such as money laundering, account takeovers, and suspicious spending pose significant challenges to financial institutions. Detecting these activities in real-time is critical for preventing financial loss and ensuring secure transactions. Existing fraud detection systems need effective and scalable methods to analyze customer and transaction data.

### Solution

The fraud detection pipeline is built using a combination of AWS services including AWS Glue, AWS Lambda, and Amazon S3, providing a scalable and efficient way to detect fraud in customer transactions.

Data Extraction and Transformation:

AWS Lambda is used to fetch blacklist data from an RDS MySQL database. The Lambda function extracts data from the src_blacklist table and performs basic transformations such as cleaning date fields, normalizing text, and handling missing values. The cleaned data is then saved as a CSV file in an S3 bucket. After uploading to S3, the Lambda function triggers an AWS Glue crawler to update the Glue catalog with the new data. The code for same is placed under lambda_cleansing_code.py file.

<img src="https://github.com/user-attachments/assets/e4b1e13d-a22c-416f-b41e-2f7288d2aa3d" width="500" />

**Lambda SC**

<img src="https://github.com/user-attachments/assets/f315eed8-ecc9-411e-9958-47929a97d601" width="500" />

**Blacklist Data in AWS RDS**


<img src="https://github.com/user-attachments/assets/10f24482-db22-4b90-8c49-743110049aa0" width="500" />

**Customer Aggregated Details**

<img src="https://github.com/user-attachments/assets/39ddbb17-563e-48a5-82b9-975dbeb66626" width="500" />

**Customer Detailed Information**


<img src="https://github.com/user-attachments/assets/53fbd66f-d281-47b1-819c-a4fc39991da1" width="500" />

**All the Crawlers used**



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

<img src="https://github.com/user-attachments/assets/7d7a604d-bf0e-44a4-8ec4-8df97af3e124" width="500" />

**Blacklisted Customer Details Data**

Risk Categorization:

Customers and transactions are classified into different risk categories (e.g., Low Risk, Medium Risk, and High Risk) based on a risk score derived from transaction characteristics. This helps prioritize further investigation.
Aggregating and Storing Data:

After the necessary transformations, the data is aggregated to provide insights into customer behaviors, such as total transaction amount, average transaction value, and the number of transactions.
The final processed data is written to Amazon S3 in Parquet format, which ensures both high performance and cost efficiency for large datasets.
Monitoring and Automation:

The system is fully automated, with Glue triggers orchestrating the data processing pipeline from data extraction to transformation and storage.
Glue crawlers update the Glue catalog with each new batch of data, enabling users to query the processed data efficiently for analytics or further processing.
By automating the extraction, transformation, and loading (ETL) of fraud detection data, this solution enhances the ability to monitor and flag suspicious activity, reducing manual intervention and improving the speed at which fraudulent transactions can be detected and mitigated.

<img src="https://github.com/user-attachments/assets/d930638b-37eb-4490-b545-28581c6949f4" width="500" />










## All the data used in above project is fabricated






