from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

rfm_dag = DAG(
    dag_id="rfm_retail_analysis",
    default_args=default_args,
    schedule_interval="0 0 1 * *",  # Run monthly
    dagrun_timeout=timedelta(minutes=60),
    description="Monthly RFM Analysis for Retail Data",
    start_date=days_ago(1),
)

run_rfm = SparkSubmitOperator(
    application="/spark-scripts/rfm_retail.py",
    conn_id="spark_main",
    task_id="rfm_analysis_task",
    dag=rfm_dag,
)

run_rfm