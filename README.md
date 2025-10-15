# spotify_etl_airflow
Spotify ETL project orchestrated through Aiflow running in docker.

A complete data engineering project that extracts Spotify listening data, transforms it, and loads it into both MySQL and Snowflake databases. The pipeline is orchestrated using Apache Airflow and allows user to use the data for visualizations.

## 🚀 Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline that:
- Extracts recent Spotify listening data from Spotify Web API
- Transforms and cleans the data for analysis
- Loads data into both MySQL (local) and Snowflake (cloud) databases
- Orchestrates the entire process using Apache Airflow running in Docker
- Provides analytics through a Power BI dashboard

## PowerBI dashboard 
A dashboard I created utilizing the data loaded into snowflake, using Direct Query in PowerBI

<!-- Add your Power BI dashboard screenshot here -->
![Power BI Dashboard](images/spotify_dash.png)

## Setup Instructions
### Prerequisites
1. Docker installation
2. Apache Airflow installation (Docker will handle this)
3. MySQL database
4. Snowflake account
5. Spotify Developer account (generate client_id and client_secret)

### requirements.txt
- apache-airflow-providers-postgres==5.11.0 
- apache-airflow-providers-http==4.5.0
- apache-airflow-providers-mysql==5.3.0
- apache-airflow-providers-snowflake==6.5.4 
- mysqlclient
- spotipy==2.25.1
- python-dotenv==1.1.1

## Installation
### Step 1: Clone the repository
### Step 2: Airflow-init
From within the root of the project directory, run:
```docker compose up airflow-init```
This might take a while

### Step 3: Docker Compose
run:
```docker compose up -d```

### Step 4: Access Airflow UI
Within your web browser, type in the URL:
```localhost:8080```

### Step 5: Create Connections
