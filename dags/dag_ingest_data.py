import csv
from airflow.decorators import dag
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pendulum import datetime

S3_BUCKET = "data-lake"
STAGING_SCHEMA = "staging"

def parse_csv_to_list(filepath):
    """ Reads a CSV file and returns a list of tuples, skipping the header. """
    with open(filepath, "r") as f:
        next(f) 
        reader = csv.reader(f)
        return [tuple(row) for row in reader]

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data_ingestion"],
)
def ingest_data_dag():

    create_staging_schema = PostgresOperator(
        task_id="create_staging_schema",
        postgres_conn_id="postgres_warehouse",
        sql=f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};"
    )

    create_staging_tables = PostgresOperator(
        task_id="create_staging_tables",
        postgres_conn_id="postgres_warehouse",
        sql="""
            CREATE TABLE IF NOT EXISTS staging.users (
                user_id TEXT,
                name TEXT,
                email TEXT,
                address TEXT,
                created_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS staging.orders (
                order_id TEXT,
                user_id TEXT,
                order_date TIMESTAMP,
                amount FLOAT,
                status TEXT
            );
            CREATE TABLE IF NOT EXISTS staging.tickets (
                ticket_id TEXT,
                user_id TEXT,
                created_at TIMESTAMP,
                resolved_at TIMESTAMP,
                status TEXT,
                priority TEXT
            );
        """
    )

    ingest_users = S3ToSqlOperator(
        task_id="ingest_users_data",
        s3_bucket=S3_BUCKET,
        s3_key="users.csv",
        sql_conn_id="postgres_warehouse",
        table=f'"{STAGING_SCHEMA}"."users"',
        aws_conn_id="minio_s3",
        parser=parse_csv_to_list,
    )

    ingest_orders = S3ToSqlOperator(
        task_id="ingest_orders_data",
        s3_bucket=S3_BUCKET,
        s3_key="orders.csv",
        sql_conn_id="postgres_warehouse",
        table=f'"{STAGING_SCHEMA}"."orders"',
        aws_conn_id="minio_s3",
        parser=parse_csv_to_list,
    )

    ingest_tickets = S3ToSqlOperator(
        task_id="ingest_tickets_data",
        s3_bucket=S3_BUCKET,
        s3_key="support_tickets.csv",
        sql_conn_id="postgres_warehouse",
        table=f'"{STAGING_SCHEMA}"."tickets"',
        aws_conn_id="minio_s3",
        parser=parse_csv_to_list,
    )

    # UPDATED DEPENDENCIES: Create schema -> Create tables -> Ingest data
    create_staging_schema >> create_staging_tables >> [ingest_users, ingest_orders, ingest_tickets]

ingest_data_dag()