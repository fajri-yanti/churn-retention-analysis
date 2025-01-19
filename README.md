# RFM (Recency, Frequency, Monetary) Retail Analysis

## Overview
This project implements an automated RFM (Recency, Frequency, Monetary) analysis pipeline for retail data using Apache Airflow and PySpark. The analysis runs monthly to segment customers based on their purchasing behavior.

## Components

### 1. Data Analysis Script (`rfm_retail.py`)
A PySpark script that:
- Reads retail transaction data from PostgreSQL
- Calculates RFM metrics:
  - Recency: Days since last purchase
  - Frequency: Number of purchases
  - Monetary: Total spending
- Writes results back to PostgreSQL

### 2. Airflow DAG (`rfm_retail_dag.py`)
An Apache Airflow DAG that:
- Schedules the RFM analysis to run monthly
- Manages the Spark job submission
- Handles task dependencies and retries

## Prerequisites
- Apache Airflow 2.x
- Apache Spark
- PostgreSQL
- PostgreSQL JDBC driver (postgresql-42.2.18.jar)
- Python 3.x

## Project Structure
```
/opt/airflow/
├── dags/
│   └── rfm_retail_dag.py
└── spark-scripts/
    └── rfm-retail.py
```

## Configuration

### 1. Environment Variables
Required environment variables in `.env`:
```
POSTGRES_CONTAINER_NAME=<postgres_host>
POSTGRES_DW_DB=<database_name>
POSTGRES_USER=<username>
POSTGRES_PASSWORD=<password>
```

### 2. Airflow Connection
Create a new Spark connection in Airflow:
```bash
airflow connections add 'spark_master' \
    --conn-type 'spark' \
    --conn-host 'spark://dataeng-spark-master' \
    --conn-port '7077'
```

Or via Airflow UI:
1. Navigate to Admin -> Connections
2. Add new connection
3. Set connection type as 'Spark'
4. Configure host and port

## Installation

1. Clone the repository and navigate to the project directory
2. Place the scripts in their respective directories:
   ```bash
   cp rfm_retail_dag.py /opt/airflow/dags/
   cp rfm-retail.py /opt/airflow/spark-scripts/
   ```
3. Set proper permissions:
   ```bash
   chmod 644 /opt/airflow/dags/rfm_retail_dag.py
   chmod 644 /opt/airflow/spark-scripts/rfm-retail.py
   ```

## Usage

The DAG is scheduled to run automatically on the first day of each month. However, you can also:

1. Trigger the DAG manually via Airflow UI:
   - Navigate to DAGs view
   - Find "rfm_retail_analysis" DAG
   - Click "Trigger DAG"

2. Test the DAG using CLI:
   ```bash
   airflow tasks test rfm_retail_analysis rfm_analysis_task 2024-01-19
   ```

## DAG Details

```python
default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

rfm_dag = DAG(
    dag_id="rfm_retail_analysis",
    default_args=default_args,
    schedule_interval="0 0 1 * *",  # Monthly schedule
    dagrun_timeout=timedelta(minutes=60),
    description="Monthly RFM Analysis for Retail Data",
    start_date=days_ago(1),
)
```

