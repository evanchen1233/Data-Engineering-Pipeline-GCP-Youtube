# Data Engineering GCP Project | YouTube Trending Data Analytics
 
## Introduction
### The goal of this project is to build data pipeline and data analysis on YouTube trending data using various tools and technologies, including GCP Storage, Python, Compute Instance, Mage Data Pipeline Tool, Apache Airflow, BigQuery, and Looker Studio.

## Project Steps/Goals
1. Create bucket on Cloud Storage and load Raw CSV and JSON files.
2. Write ETL and DAG code in Python and test on Jupyter Notebook .
3. Create, setup Apache Airflow and load ETL and DAG code on VM.
4. Load to Bigquery as Data Warehouse. Futher analysis with sql queries.
5. Collect subset of data warehouse for BI dashboard and load to Looker Studio.
6. Create dashboard on Looker Studio.
7. Use Mage AI instead of Apache Airflow and compare Pros and Cons.

## Architecture
![Untitled-2023-05-21-1857](https://github.com/evanchen1233/Data-Engineering-Pipeline-GCP-Project-Youtube-Trending-Data/assets/101177476/59bbfdba-de9b-4cf5-b4a8-8c45aebd17b4)

## Dashboard
![Dashboard](https://github.com/evanchen1233/Data-Engineering-Pipeline-GCP-Project-Youtube-Trending-Data/assets/101177476/dd7b527c-f410-4290-82ca-a9d92e9947bc)

## Technology Used

* Programming Language - Python

Google Cloud Platform
1. Cloud Storage: Cloud Storage is a managed service for storing unstructured data.
2. Compute Instance: Google's virtual machine service.
3. BigQuery: BigQuery is a serverless and cost-effective enterprise data warehouse that works across clouds and scales with your data.
4. Looker Studio: Looker Studio is a BI tool that turns your data into informative, easy to read, easy to share, and fully customizable dashboards and reports

Workflow Management
Apache Airflow - Apache Airflow is an open-source platform for authoring, scheduling and monitoring data and computing workflows. \
Modern Data Pipeine Tool - https://www.mage.ai/ - Modern alternative to Apache Airflow 

## Dataset Used

This Kaggle dataset contains statistics (CSV files) on daily popular YouTube videos over the course of many months. There are up to 200 trending videos published every day for many locations. The data for each region is in its own file. The video title, channel title, publication time, tags, views, likes and dislikes, description, and comment count are among the items included in the data. A category_id field, which differs by area, is also included in the JSON file linked to the region.

https://www.kaggle.com/datasets/datasnaek/youtube-new
