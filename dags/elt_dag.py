# -*- coding: utf-8 -*-
import datetime
import json
import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import \
    S3ToRedshiftOperator
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator, SQLTableCheckOperator)
from utils.airtable_to_s3 import load_process_airtable_to_s3

with open("/elt-example-dag/dags/params.json", "r") as file:
    params = json.load(file)
TABLE_NAMES = params["table_names"]
BUCKET_NAME = params["bucket_name"]
REDSHIFT_SCHEMA = params["redshift_schema"]

dag_id = "my_pipeline"

default_args = {
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=10),
    "depends_on_past": False,
}

dag = DAG(
    dag_id,
    start_date=pendulum.datetime(2023, 5, 1, tz="UTC"),
    default_args=default_args,
    description="An example DAG which fetches data from airtable, writes into S3 buckets, ingests into redshift tables and transforms data into final table",
    schedule_interval="@daily",
    max_active_runs=1,
    max_active_tasks=3,
)

transform_to_event_sequence = SQLExecuteQueryOperator(
    task_id="transform_to_event_sequence",
    sql="event_sequence.sql",
    params={"event_schema": REDSHIFT_SCHEMA, "table_names": TABLE_NAMES},
    conn_id="redshift_default",
    dag=dag,
)

table_checks = SQLTableCheckOperator(
    task_id="sequence_table_data_quality_checks",
    conn_id="redshift_default",
    table="app events",
    checks={
        "row_count_check": {"check_statement": "COUNT(*) >= 3"},
        "average_happiness_check": {
            "check_statement": "AVG(bird_happiness) >= 9",
            "partition_clause": "observation_year >= 2021",
        },
    },
)

for event_type in TABLE_NAMES:
    src_table = event_type["source"]
    dest_table = event_type["target"]
    airtable_to_s3 = PythonOperator(
        task_id=f"load_{dest_table}_to_s3",
        provide_context=True,
        python_callable=load_process_airtable_to_s3,
        op_kwargs={"aws_conn": "aws_default", "bucket_name": BUCKET_NAME, "table_name": src_table},
        dag=dag,
    )
    s3_to_redshift = S3ToRedshiftOperator(
        task_id=f"copy_{dest_table}_to_redshift",
        redshift_conn_id="redshift_default",
        s3_bucket=BUCKET_NAME,
        s3_key="{{{{ ti.xcom_pull('load_{}_to_s3') }}}}".format(dest_table),
        schema=REDSHIFT_SCHEMA,
        table=dest_table,
        copy_options=["csv", "delimiter ';'", "ignoreheader 1", "timeformat as 'auto'"],
        method="UPSERT",
        upsert_keys=["ID"],
        dag=dag,
    )
    airtable_to_s3 >> s3_to_redshift >> transform_to_event_sequence

transform_to_event_sequence >> table_checks
