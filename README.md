# The data pipeline for processing user event data from a recruitment site

## Objective
The company seeks to leverage user interaction data from the recruitment platform to drive business growth. Given the immense volume of daily user logs, this project was initiated to build a data pipeline that can efficiently capture and process candidate activity in a near real-time with Kafka, Spark. This provides the foundation for analytics teams to evaluate user behavior, understand trends, and deliver timely insights to shape business development strategies.

### Technology
All hosted by Docker:
- Airflow
- Kafka
- Spark 
- Python
- Linux
- PowerBI/Grafana (optional)

## System
![image](https://github.com/lamtt14/DE_project/blob/main/assets/diagram.jpg?raw=true)

Raw user data on the recruitment site is generated through simulation and then ingested into Cassandra via Kafka. The data was processed by Spark, then combined with data from MySQL before loaded into MySQL

### Raw data
Raw data schema:
```sh
.
root
 |-- create_time: string (nullable = false)
 |-- bid: integer (nullable = true)
 |-- bn: string (nullable = true)
 |-- campaign_id: integer (nullable = true)
 |-- cd: integer (nullable = true)
 |-- custom_track: string (nullable = true)
 |-- de: string (nullable = true)
 |-- dl: string (nullable = true)
 |-- dt: string (nullable = true)
 |-- ed: string (nullable = true)
 |-- ev: integer (nullable = true)
 |-- group_id: integer (nullable = true)
 |-- id: string (nullable = true)
 |-- job_id: integer (nullable = true)
 |-- md: string (nullable = true)
 |-- publisher_id: integer (nullable = true)
 |-- rl: string (nullable = true)
 |-- sr: string (nullable = true)
 |-- ts: string (nullable = true)
 |-- tz: integer (nullable = true)
 |-- ua: string (nullable = true)
 |-- uid: string (nullable = true)
 |-- utm_campaign: string (nullable = true)
 |-- utm_content: string (nullable = true)
 |-- utm_medium: string (nullable = true)
 |-- utm_source: string (nullable = true)
 |-- utm_term: string (nullable = true)
 |-- v: integer (nullable = true)
 |-- vp: string (nullable = true)
```
### Processing data 
Read and review the data recording user actions in the log data, notice that there are actions with analytical value in the column ```["custom_track"]``` including: ```clicks, conversion, qualified, unqualified```.
Processing raw data to obtain valuable clean data:

- Filter actions with analytical value in column ```["custom_track"]``` including: ```clicks, conversion, qualified, unqualified```.
- Remove null values, replace with 0 to be able to calculate.
- Using PySpark to write Spark jobs and process data efficiently.
- Processed data is saved to MySQL for storage and further in-depth analysis.
- Using Airflow to schedule Spark jobs.

### Processed data 
```sh
root
 |-- job_id: integer (nullable = true)
 |-- dates: timestamp (nullable = true)
 |-- hours: integer (nullable = true)
 |-- disqualified_application: integer (nullable = true)
 |-- qualified_application: integer (nullable = true)
 |-- conversion: integer (nullable = true)
 |-- company_id: integer (nullable = true)
 |-- group_id: integer (nullable = true)
 |-- campaign_id: integer (nullable = true)
 |-- publisher_id: integer (nullable = true)
 |-- bid_set: double (nullable = true)
 |-- clicks: integer (nullable = true)
 |-- impressions: string (nullable = true)
 |-- spend_hour: double (nullable = true)
 |-- sources: string (nullable = true)
 |-- latest_update_time: timestamp (nullable = true)
```

### Change data capture: Timestamp-based 
The data pipeline architecture using a simple timestamp-based CDC to recognize the newest records in Cassandra and then trigger spark jobs to process and load that records to MySQL for further in-depth analysis

### Airflow
SparkSubmitOperator, BranchPythonOperator, PythonOperator are used for execution and are set to run every day at 6 AM

### Challenge: Visualization with PowerBI/Grafana
The scope of this project was focused on building the data pipeline and implementing data processing workflows. It did not cover data analysis or insight generation. For deeper exploration and interpretation, visualization tools can be employed to perform advanced analysis.