import json
import pendulum
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# GLOBALS
SCALA_VERSION = '2.12'
DELTA_CORE_VERSION = '2.3.0'
AWS_HADOOP_VERSION = '3.3.2'


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    params={
        "job_name": None,
        "application": Param("/airflow/jobs/ingestor.py"),
        "source_dir": None
    },
    tags=["spark"],
)
def submit_spark_job():

    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        application="{{ params.application }}",
        packages=f'io.delta:delta-core_{SCALA_VERSION}:{DELTA_CORE_VERSION},org.apache.hadoop:hadoop-aws:{AWS_HADOOP_VERSION}',
        conf={
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.hadoop.fs.s3a.access.key': 'minio',
            'spark.hadoop.fs.s3a.secret.key': 'minio123',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
        },
        application_args=["{{ params.job_name }}", "{{  params.source_dir }}"]
    )


submit_spark_job()
