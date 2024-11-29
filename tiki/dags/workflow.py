
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from result import crawl_data
from process_data import process_all_files
from kafka_producer import produce_to_kafka
from kafka_consumer import consume_from_kafka

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Định nghĩa DAG
with DAG(
    dag_id='tiki_data_pipeline',
    default_args=default_args,
    description='Pipeline xử lý dữ liệu từ Tiki',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 27),
    catchup=False,
) as dag:

    # Task 1: Crawl dữ liệu từ Tiki
    crawl_task = PythonOperator(
        task_id='crawl_tiki_data',
        python_callable=crawl_data,
    )

    # Task 2: Xử lý dữ liệu đã crawl
    process_task = PythonOperator(
        task_id='process_tiki_data',
        python_callable=process_all_files,
        op_kwargs={
            'input_dir': '/opt/airflow/dags/data',
            'output_dir': '/opt/airflow/dags/processed_data',
        },
    )

    # Task 3: Gửi dữ liệu đã xử lý lên Kafka
    produce_task = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_to_kafka,
        op_kwargs={
            'input_dir': '/opt/airflow/dags/processed_data',
            'kafka_server': 'kafka:9092',
            'topic': 'tiki_products',
        },
    )

    # Task 4: Tiêu thụ dữ liệu từ Kafka và lưu vào PostgreSQL
    consume_task = PythonOperator(
        task_id='consume_from_kafka',
        python_callable=consume_from_kafka,
        op_kwargs={
            'batch_size': 100,  # Kích thước batch để ghi vào PostgreSQL
        },
    )

    # Định nghĩa thứ tự thực hiện các task
    crawl_task >> process_task >> produce_task >> consume_task
