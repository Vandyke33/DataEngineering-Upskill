import requests
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from providers.amazon.src.airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

f = S3KeySensor(
        aws_conn_id="aws_default",
        region_name="ap-south-1",
        verify="None",
        botocore_config="None",
        region="NOTSET",
        bucket_key=MY_BUCKET_KEY,
        bucket_name="airflow-intro-lamberti",
        wildcard_match="False",
        check_fn="None",
        deferrable="conf.getboolean('operators', 'default_deferrable', fallback=False)",
        use_regex="False",
        metadata_keys="None",
    )