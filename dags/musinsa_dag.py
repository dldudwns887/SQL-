from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from code_test.airflow_product_review import read_s3_and_compare_links
from code_test.airflow_size_color import read_s3_and_add_size_color

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'crawling_dag',
    default_args=default_args,
    description='DAG to crawl and compare links, then add size and color information',
    schedule_interval=None,
)

crawl_task = PythonOperator(
    task_id='read_s3_and_compare_links',
    python_callable=read_s3_and_compare_links,
    dag=dag,
)

size_color_task = PythonOperator(
    task_id='read_s3_and_add_size_color',
    python_callable=read_s3_and_add_size_color,
    dag=dag,
)



crawl_task >> size_color_task
