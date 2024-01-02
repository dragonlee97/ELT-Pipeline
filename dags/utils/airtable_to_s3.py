import os
from airflow.hooks.S3_hook import S3Hook
from pyairtable import Table
from utils.process_records import RecordProcessor
from airflow.models import Variable

AIRTABLE_API_KEY = Variable.get("AIRTABLE_API_KEY")

def load_process_airtable_to_s3(aws_conn, bucket_name: str, table_name: str, **kwargs):
    airtable = Table(AIRTABLE_API_KEY, os.getenv("AIRTABLE_BASE_ID"), table_name)
    processor = RecordProcessor()
    csv_file = processor.transform_to_csv(airtable, kwargs["ds"])
    file_name = kwargs["ds_nodash"] + "/" + table_name + ".csv"
    s3_hook = S3Hook(aws_conn_id=aws_conn)
    s3_hook.load_file_obj(file_obj=csv_file, key=file_name, bucket_name=bucket_name, replace=False, encrypt=False)
    return file_name
