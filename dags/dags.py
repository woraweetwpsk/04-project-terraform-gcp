from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta
import requests
import os

URL_RAW = ""
AIRFLOW_RAW_PATH = "/home/airflow/gcs/data/"
BUCKET_NAME = "de-project-gcp-bucket"
RAW_FILE_NAME = "raw.csv"
GCS_RAW_DST = f"raw/{RAW_FILE_NAME}"
GCS_RAW_PATH = f"gs://{BUCKET_NAME}/{GCS_RAW_DST}"
PROJECT_ID = ""
DATAPROC_CLUSTER_NAME = "gcp-project-dataproc-cluster"
SPARK_OUTPUT = f"gs://{BUCKET_NAME}/stage/"


def _download_raw_data(URL_RAW, OUTPUT_PATH):
    #Download Raw Data from URL to Airflow Path
    response = requests.get(URL_RAW)
    if response.status_code == 200:
        path = os.path.join(OUTPUT_PATH, RAW_FILE_NAME)
        with open(path, "wb") as file:
            file.write(response.content)
            print(f"Raw file is saved in {path} complete")
    else:
        print("Failed to download raw file")

default_args = {
    "owner" : "Worawee",
    "depends_on_past" : False,
    "start_date" : datetime(2024,8,23),
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5)
}

spark_job = {
    "reference" : {"project_id" : PROJECT_ID},
    "placement" : {"cluster_name" : DATAPROC_CLUSTER_NAME},
    "pyspark_job" : {
        "main_python_file_uri" : f"gs://{BUCKET_NAME}/spark/spark_job.py",
        "args":[
            "--input_path", GCS_RAW_PATH,
            "--output_path", SPARK_OUTPUT
        ]
    }
}

with DAG(
    dag_id="tranform_dag",
    default_args=default_args,
    schedule_interval="@once"
) as dag:

#Download Raw Date to Local Airflow
    download_raw_data = PythonOperator(
        task_id = "download_raw_data",
        python_callable = _download_raw_data,
        op_kwargs = {"URL_RAW" : URL_RAW,
                     "OUTPUT_PATH" : AIRFLOW_RAW_PATH
                     }
    )

#Upload Raw Data from Local Airflow to GCS raw bucket
    airflow_to_gcs_bucket = LocalFilesystemToGCSOperator(
        task_id = "airflow_to_gcs_bucket",
        src=os.path.join(AIRFLOW_RAW_PATH, RAW_FILE_NAME),
        dst=GCS_RAW_DST,
        bucket=BUCKET_NAME
        )

#Spark Transform Data to GCS stage bucket
    pyspark_job = DataprocSubmitJobOperator(
        task_id = "pyspark_job",
        job = spark_job,
        region = "asia-south2",
        project_id = PROJECT_ID
    )
    
#Uplod to BigQuery
    upload_to_bigquey = GCSToBigQueryOperator(
        task_id = "upload_to_bigquery",
        bucket = BUCKET_NAME,
        source_objects = ["stage/*.parquet"],
        source_format = "PARQUET",
        destination_project_dataset_table = "project_gcp_dataset.final",
        write_disposition="WRITE_TRUNCATE"
    )

download_raw_data >> airflow_to_gcs_bucket >> pyspark_job >> upload_to_bigquey