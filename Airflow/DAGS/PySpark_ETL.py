from datetime import datetime,timedelta , date 
from airflow import models,DAG 
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators import BashOperator 
from airflow.models import *
from airflow.utils.trigger_rule import TriggerRule

current_date = str(date.today())

PROJECT_ID = "Pyspark-ETL"

PYSPARK_BUCKET = "gs://dataproc-staging-us-central1-952657444860-ifr3oxy6"

gcs_bucket = 'weekly_sales_data'

PROJECT_ID_DS = "gcp-322704"

#PYSPARK_JOB = PYSPARK_BUCKET + "/notebooks/jupyter/etl_weekly_data.py"

DEFAULT_DAG_ARGS = {
    'owner':"airflow",
    'depends_on_past' : False,
    "start_date":datetime.utcnow(),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries": 1,
    "retry_delay":timedelta(minutes=5),
    "project_id":PROJECT_ID,
    "scheduled_interval":"30 2 * * *"
}

with DAG("Wkly_cust_data_load",default_args=DEFAULT_DAG_ARGS) as dag : 

    bq_data_clean = BigQueryDeleteTableOperator(
    task_id = 'Bigquery_tables_cleanup',
    deletion_dataset_table=f"{PROJECT_ID_DS}.data_analysis_weekly.weekly_cust_data"
    )

    delete_tranformed_files = BashOperator(
        task_id = "GCS_bucket_cleanup",
        bash_command = "gsutil -m rm -r gs://weekly_sales_data/ics_weekly_data/*"
    )
    
    PYSPARK_JOB = {
    "reference": {"project_id": 'gcp-322704'},
    "placement": {"cluster_name": 'spark-etl-cluster'},
    "pyspark_job": {"main_python_file_uri": 'gs://dataproc-staging-us-central1-952657444860-ifr3oxy6/notebooks/jupyter/etl_weekly_data.py'},
    }
    
    submit_pyspark = DataprocSubmitJobOperator(
        task_id = "Run_pyspark_job",
        job = PYSPARK_JOB,
        location="us-central1",
        project_id= 'gcp-322704'
    )

    bq_load_cust_data = GoogleCloudStorageToBigQueryOperator(

        task_id = "Load_data_into_Table",
        bucket=gcs_bucket,
        source_objects=['ics_weekly_data/*'],
        source_objects_task_id='task-id-of-previos-task',
        destination_project_dataset_table = f'{PROJECT_ID_DS}:data_analysis_weekly.weekly_cust_data',
        autodetect=True,
        create_disposition='CREATE_IF_NEEDED',
        source_format='NEWLINE_DELIMITED_JSON',
        skip_leading_rows=0,
        write_disposition='WRITE_APPEND',
        max_bad_records=0
    )
    
    data_count = BigQueryCheckOperator(
    task_id = 'check_data_count',
    use_legacy_sql=False,
    sql = f'SELECT count(*) FROM `{PROJECT_ID_DS}.data_analysis_weekly.weekly_cust_data`'
    )

    bq_data_clean.dag = dag
    
    delete_tranformed_files.set_upstream(bq_data_clean)
    
    submit_pyspark.set_upstream(delete_tranformed_files)

    bq_load_cust_data.set_upstream(submit_pyspark)
    
    data_count.set_upstream(bq_load_cust_data)


