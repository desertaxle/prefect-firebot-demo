import os
from datetime import timedelta

import pandas as pd
from prefect import Flow, Parameter, task
from prefect.tasks.aws.s3 import S3Upload
from prefect.tasks.firebolt.firebolt import FireboltQuery
from prefect.tasks.secrets.base import PrefectSecret
from prefect.run_configs.local import LocalRun
from prefect.storage.local import Local

s3_upload = S3Upload(max_retries=3, retry_delay=timedelta(seconds=5))
firebolt_query = FireboltQuery(max_retries=3, retry_delay=timedelta(seconds=5))


@task
def convert_csv_to_parquet(csv_file_path):
    df = pd.read_csv(csv_file_path)
    return df.to_parquet()


@task
def create_firebolt_create_external_table_query(
    external_table_name, s3_bucket, aws_credentials
):
    return f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {external_table_name} (
            transaction_number TEXT,
            purchase_date      TEXT,
            product_number     TEXT,
            product_name       TEXT,
            price              LONG,
            quantity           INT,
            revenue            LONG,
            customer_number    TEXT,
            country            TEXT
            )
        URL = 's3://{s3_bucket}/'
        CREDENTIALS = ( AWS_KEY_ID = '{aws_credentials.ACCESS_KEY}' AWS_SECRET_KEY = '{aws_credentials.SECRET_ACCESS_KEY}')
        OBJECT_PATTERN = '*.parquet'
        TYPE = (PARQUET);
    """


@task
def create_firebolt_create_fact_table_query(fact_table_name):
    return f"""
    CREATE FACT TABLE IF NOT EXISTS {fact_table_name} (
        transaction_number TEXT,
        purchase_date      TEXT,
        product_number     TEXT,
        product_name       TEXT,
        price              LONG,
        quantity           INT,
        revenue            LONG,
        customer_number    TEXT,
        country            TEXT
    )
    PRIMARY INDEX
    transaction_number,
    product_number;
    """


@task
def create_firebolt_insert_query(external_table_name, fact_table_name):

    return f"""
    INSERT INTO {fact_table_name} SELECT * FROM {external_table_name};
    """


with Flow("Load data into Firebolt", storage=Local(), run_config=LocalRun()) as flow:
    firebolt_database = Parameter("firebolt_database")
    firebolt_engine_name = Parameter("firebolt_engine_name")
    s3_bucket = Parameter("s3_bucket")
    csv_file_path = Parameter("csv_file_path")
    fact_table_name = Parameter("fact_table_name")
    external_table_name = Parameter("external_table_name")

    firebolt_username = PrefectSecret("FIREBOLT_USERNAME")
    firebolt_password = PrefectSecret("FIREBOLT_PASSWORD")
    aws_credentials = PrefectSecret("AWS_CREDENTIALS")

    parquet_data = convert_csv_to_parquet(csv_file_path)
    parquet_data_upload = s3_upload(
        data=parquet_data, bucket=s3_bucket, key="business_sales_transaction.parquet"
    )

    create_external_table_query = create_firebolt_create_external_table_query(
        external_table_name, s3_bucket, aws_credentials
    )
    create_fact_table_query = create_firebolt_create_fact_table_query(fact_table_name)
    insert_query = create_firebolt_insert_query(external_table_name, fact_table_name)

    firebolt_external_table_creation = firebolt_query(
        database=firebolt_database,
        engine_name=firebolt_engine_name,
        username=firebolt_username,
        password=firebolt_password,
        query=create_external_table_query,
    )

    firebolt_fact_table_creation = firebolt_query(
        database=firebolt_database,
        engine_name=firebolt_engine_name,
        username=firebolt_username,
        password=firebolt_password,
        query=create_fact_table_query,
    )

    firebolt_insert_into = firebolt_query(
        database=firebolt_database,
        engine_name=firebolt_engine_name,
        username=firebolt_username,
        password=firebolt_password,
        query=insert_query,
        upstream_tasks=[
            parquet_data_upload,
            firebolt_external_table_creation,
            firebolt_fact_table_creation,
        ],
    )

if __name__ == "__main__":
    flow.run(
        parameters={
            "firebolt_database": "business_sales_transactions",
            "firebolt_engine_name": "business_sales_transactions_general_purpose",
            "fact_table_name": "business_sales_transaction",
            "external_table_name": "ex_business_sales_transaction",
            "s3_bucket": "prefect-firebolt-test-bucket",
            "csv_file_path": f"{os.getcwd()}/business_sales_transaction.csv",
        }
    )
