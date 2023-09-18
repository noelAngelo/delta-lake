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
        "source_name": Param(default="accounts", title="Source name"),
        "source_bucket": Param(default="sources-prod-up", title="Source bucket"),
        "source_path": Param(default="accounts", title="Source path"),
        "target_bucket": Param(default="delta", title="Target bucket"),
        "target_schema": Param(default="up", title="Target schema"),
        "target_checkpoint": Param(default="config-ctrlframework", title="Target checkpoint"),
        "application": Param("/airflow/jobs/ingestor.py", title="Python application"),
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
        application_args=[
            "{{ params.source_bucket }}",
            "{{ params.source_name }}",
            "{{ params.source_path }}",
            "{{ params.target_bucket }}",
            "{{ params.target_schema }}",
            "{{ params.target_checkpoint }}",
        ]
    )


submit_spark_job()
