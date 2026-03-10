from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "analytics_engineer",
    "retries": 1,
}

with DAG(
    dag_id="lakehouse_medallion_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Orchestration Medallion Spark + Delta",
) as dag:

    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command="docker exec spark-lakehouse spark-submit /opt/spark/jobs/bronze_ingestion.py"
    )

    silver_task = BashOperator(
        task_id="silver_transformation",
        bash_command="docker exec spark-lakehouse spark-submit /opt/spark/jobs/silver_transformation.py"
    )

    gold_task = BashOperator(
        task_id="gold_modeling",
        bash_command="docker exec spark-lakehouse spark-submit /opt/spark/jobs/gold_modeling_pro.py"
    )

    bronze_task >> silver_task >> gold_task