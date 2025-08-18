## Advanced-Real-Time-Data-Pipeline-and-Analytical-Processing
Designed and implemented a scalable real-time data pipeline that monitors a directory for incoming data, processes it based on specific criteria, and stores the transformed data in a relational database for further analysis. Considered factors such as data integrity, performance, and scalability.

## Technology Stack

- **Apache Spark 3.5**: Real-time streaming and data processing.
- **PySpark**: Python API for Spark for transformation and analytics.
- **Spark Streaming**: Continuous ingestion of real-time data.
- **PostgreSQL**: Stores transformed data for persistence and querying.
- **S3 / MinIO**: Raw data storage and retrieval.
- **Docker**: Containerized Spark environment for consistent deployment.
- **Boto3 / AWS SDK**: Integration with S3 for reading/writing data.

# End-to-End ETL Architechture for Production ready.

![Architecture](Architecturediagram.png)
## Architecture Overview

The architecture follows these steps:

1. **Data Source (1)**  
   - Files or sensor data are generated and stored in a local folder or S3-compatible storage.

2. **Data Ingestion (2)**  
   - Spark Streaming monitors the source bucket/folder and ingests data in real-time.

3. **Data Transformation (3)**  
   - Incoming data is cleaned, formatted, and transformed using Spark Streaming and PySpark transformations.

4. **Storing Transformed Data (4)**  
   - Transformed data is written into PostgreSQL for persistence and further querying.

5. **Storing Raw Data (5)**  
   - Tranformed data is optionally stored in a data lake or warehouse (e.g., PostgreSQL, HDFS) for aggregation.

6. **Aggregation & Analytics (6)**  
   - Aggregations, summaries, or feature calculations are performed using PySpark.
   - Aggregated results can be used for dashboards, reporting, or machine learning.

## About Dataset Link
This dataset simulates real-world smart farming operations powered by IoT sensors and satellite data. It captures environmental and operational variables that affect crop yield across 500 farms located in regions like India, the USA, and Africa.
[Link](https://www.kaggle.com/datasets/atharvasoundankar/smart-farming-sensor-data-for-yield-prediction)

Author :- Sudhanshu Pukale
