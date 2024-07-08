from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
from pyspark.sql import SparkSession
import json
from dotenv import load_dotenv
import shutil
import os

load_dotenv()

spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

def fetch_data(api_url):
    print('Starting data extraction')
    response = requests.get(api_url)
    print("Response status code:", response.status_code)
    data = response.json()
    raw_data_path = "/tmp/data.json"
    with open(raw_data_path, "w") as f:
        json.dump(data, f)
    return raw_data_path

def transform_data(ti):
    print('Starting data transformation')
    raw_data_path = ti.xcom_pull(task_ids='Extract_from_API')
    df = spark.read.json(raw_data_path)
    transformed_df = df.filter(df['id'].isNotNull())
    transformed_df = transformed_df.dropna()
    transformed_df = transformed_df.select('id', 'launch', 'spaceTrack', 'version', 'height_km', 'latitude', 'longitude', 'velocity_kms')
    transformed_path = '/tmp/transformed_data.json'
    transformed_df.write.mode('overwrite').json(transformed_path)

    zip_path = transformed_path + '.zip'
    shutil.make_archive(transformed_path, 'zip', os.path.dirname(transformed_path), os.path.basename(transformed_path))
    return zip_path

def upload_to_s3(ti, bucket_name, object_name):
    print('Starting data upload to S3')
    file_path = ti.xcom_pull(task_ids='Transform_data')

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"File uploaded to S3://{bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
}

api_url = "https://api.spacexdata.com/v4/starlink"
bucket_name = "pipeline-api-data"
object_name = "tmp/transformed_data.json"

with DAG(
    dag_id='data_pipeline',
    description='A simple data pipeline DAG',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['xcom', 'python']
) as dag:

    task_extract = PythonOperator(
        task_id='Extract_from_API',
        python_callable=fetch_data,
        op_kwargs={'api_url': api_url},
    )

    task_transform = PythonOperator(
        task_id='Transform_data',
        python_callable=transform_data,
    )

    task_load = PythonOperator(
        task_id='Load_to_S3',
        python_callable=upload_to_s3,
        op_kwargs={'bucket_name': bucket_name, 'object_name': object_name},
    )

    task_extract >> task_transform >> task_load