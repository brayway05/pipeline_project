Airflow DAG code is in the dags folder.


## DAG contains

1. API call to fetch some public data
2. Apache Spark code to do basic clean and transform on the json data from API
3. Code to upload to personal S3 bucket where a lambda function will generate a pre-signed URL so that my React portfolio website can display it whenever someone views my website
 - When the data is pulled from the S3 bucket it is cached on the website browser so the user doesn't accidentally make lots of requests to my S3 bucket. 
