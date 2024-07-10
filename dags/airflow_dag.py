from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from datetime import datetime, timedelta

def fetch_data(api_url):
    import requests
    import json

    print(f'Starting data extraction with url: {api_url}')
    response = requests.get(api_url)
    print("Response status code:", response.status_code)
    data = response.json()
    raw_data_path = "/tmp/data.json"
    with open(raw_data_path, "w") as f:
        json.dump(data, f)
    return raw_data_path

def transform_data(ti):
    import os
    from pyspark.sql import SparkSession
    import shutil

    spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

    raw_data_path = ti.xcom_pull(task_ids='Extract_from_API')
    print(f'Starting data transformation with path: {raw_data_path}')
    df = spark.read.json(raw_data_path)
    transformed_df = df.filter(df['id'].isNotNull())
    transformed_df = transformed_df.dropna()
    transformed_df = transformed_df.select('id', 'launch', 'spaceTrack', 'version', 'height_km', 'latitude', 'longitude', 'velocity_kms')
    transformed_path = '/tmp/transformed_data.json'
    transformed_df.write.mode('overwrite').json(transformed_path)

    zip_path = transformed_path + '.zip'
    shutil.make_archive(transformed_path, 'zip', os.path.dirname(transformed_path), os.path.basename(transformed_path))
    return zip_path

def upload_to_s3(file_path):
    from dotenv import load_dotenv
    import boto3
    import os
    from airflow.exceptions import AirflowException
    from airflow.models.connection import Connection

    load_dotenv()

    bucket_name = "pipeline-api-data"
    object_name = "tmp/transformed_data"

    print(f'Starting data upload to S3 with path: {file_path}')

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_DEFAULT_REGION')

    conn = Connection(
        conn_id="s3-connection",
        conn_type="aws",
        login=aws_access_key_id,  # Reference to AWS Access Key ID
        password=aws_secret_access_key,  # Reference to AWS Secret Access Key
        extra={
            "region_name": aws_region,
        },
    )

    env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
    conn_uri = conn.get_uri()
    print(f"{env_key}={conn_uri}")

    os.environ[env_key] = conn_uri
    print(conn.test_connection())  # Validate connection credentials.

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"File uploaded to S3://{bucket_name}/{object_name}")
    except Exception as e:
        error_message = f"Error uploading file to S3: {e}"
        print(error_message)
        raise AirflowException(error_message)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
}

api_url = "https://api.spacexdata.com/v4/starlink"

with DAG(
    dag_id='data_pipeline',
    description='A simple data pipeline DAG',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['xcom', 'python']
) as dag:

    task_extract = PythonVirtualenvOperator(
        task_id='Extract_from_API',
        python_callable=fetch_data,
        op_kwargs={'api_url': api_url},
    )

    task_transform = PythonOperator(
        task_id='Transform_data',
        python_callable=transform_data,
    )

    task_load = PythonVirtualenvOperator(
        task_id='Load_to_S3',
        python_callable=upload_to_s3,
        op_kwargs={'file_path': '{{ ti.xcom_pull(task_ids="Transform_data") }}'}
    )
    

    task_extract >> task_transform >> task_load