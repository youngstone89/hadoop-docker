from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import \
    SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='A simple Spark job',
    schedule_interval=timedelta(days=1),
)

spark_job = SparkSubmitOperator(
    task_id='spark_submit_job',
    application='/opt/spark_jobs/spark_job_app.py',  # Path to your Spark job
    conn_id='spark_local',  # Connection ID from Airflow connections
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
        'spark.executor.cores': '1',
        'spark.driver.cores': '1',
        'spark.driver.extraJavaOptions': '-Dlog4j.configuration=file:///opt/spark/conf/log4j.properties',
        'spark.executor.extraJavaOptions': '-Dlog4j.configuration=file:///opt/spark/conf/log4j.properties',

    },
    dag=dag
)

spark_job
