import os
import time

import airflow.providers.amazon.aws.hooks.s3 as s3
import pandas as pd
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago

# Define variables
GLO_POSTGRES_DB = 'postgres_db'
S3_GLO = 'aws_default'
PREFIX = 'airflow'
TEMP_PATH = Variable.get('temp_path')
S3_BUCKET_NAME = Variable.get('s3_data_lake')


default_args = {
    "owner": "dbnguyen",
    "email": ["dbnguyen@glo.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="0 */12 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["User", "Upload s3 files"],
    max_active_runs=1
)
def Pipeline_Airflow():

    def check_file_exists_s3(s3_hook, bucket_name: str, file_name: str) -> bool:
        """Check if file exists in S3 bucket."""
        return s3_hook.check_for_key(file_name, bucket_name=bucket_name)

    @task
    def extract_data():
        pg_hook = PostgresHook(postgres_conn_id=GLO_POSTGRES_DB)
        connection = pg_hook.get_conn()
        query = "SELECT * FROM public.users LIMIT 10;"
        print("Query: ", query)

        # create filename by date
        date_str = time.strftime("%Y%m%d")
        file_name = os.path.join(TEMP_PATH, f"{date_str}.parquet")

        # Extract data from database
        df = pd.read_sql(query, connection)

        # Save file by parquet format
        df.to_parquet(file_name, engine="pyarrow")

        return file_name

    @task
    def upload_to_s3(path_local_file: str):
        if not path_local_file:
            print("File not found")
            return
        # Create hook connect to S3
        s3_hook = s3.S3Hook(S3_GLO)

        # Get filename
        file_name = os.path.basename(path_local_file)
        s3_path = f"{PREFIX}/{file_name}"

        print(f"Uploading file {file_name} to S3 bucket: {S3_BUCKET_NAME}")

        try:
            if check_file_exists_s3(s3_hook, S3_BUCKET_NAME, s3_path):
                print(
                    f"Object {file_name} already exists in bucket {S3_BUCKET_NAME}. Skipping...")
                return

            # Upload file to S3
            s3_hook.load_file(path_local_file, s3_path,
                              bucket_name=S3_BUCKET_NAME, replace=True)
            print(f"Uploaded file {file_name} to S3 bucket successfully")
        except Exception as e:
            print(f"Error uploading file {file_name} to S3: {e}")

    ###############################
    file_name = extract_data()
    upload_to_s3(file_name)


dag = Pipeline_Airflow()