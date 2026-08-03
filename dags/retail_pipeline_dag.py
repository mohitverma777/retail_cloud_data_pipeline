from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import snowflake.connector
from datetime import datetime, timedelta
from faker import Faker
import random
import json
import boto3
import os

fake = Faker()

# ──── ENTER YOUR AWS CREDENTIALS ────
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')

# ──── ENTER YOUR SNOWFLAKE CREDENTIALS ────
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT') 
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def generate_and_upload_to_s3():
    """Generates local data batches and streams them into AWS S3."""
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Structured Transactions Batch
    txn_data = []
    for _ in range(10):
        txn_data.append({
            "transaction_id": f"TXN-{random.randint(10000, 99999)}",
            "customer_name": fake.name(),
            "product_category": random.choice(["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports"]),
            "price": round(random.uniform(10.0, 500.0), 2),
            "quantity": random.randint(1, 5),
            "payment_method": random.choice(["Credit Card", "Debit Card", "UPI", "Cash"]),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    txn_file = f"transactions_{timestamp_str}.json"
    with open(txn_file, "w") as f:
        for record in txn_data:
            f.write(json.dumps(record) + "\n")

    # 2. Unstructured Chat Logs Batch
    chat_id = f"CHAT-{random.randint(1000, 9999)}"
    chat_payload = {
        "chat_id": chat_id,
        "text_log": f"customer:{fake.sentence()} Agent:{fake.sentence()}",
        "origin_device": random.choice(["iOS", "Android", "Web"])
    }
    chat_file = f"chat_{timestamp_str}.json"
    with open(chat_file, "w") as f:
        json.dump(chat_payload, f)

    # 3. Stream Outbound to AWS S3 Bucket
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    
    s3_client.upload_file(txn_file, AWS_BUCKET_NAME, f"transactions/raw/{txn_file}")
    s3_client.upload_file(chat_file, AWS_BUCKET_NAME, f"chats/raw/{chat_file}")
    
    os.remove(txn_file)
    os.remove(chat_file)
    print("🚀 Data successfully generated and staged inside Amazon S3 Data Lake!")

def bulk_load_s3_to_snowflake():
    """Triggers Snowflake parallel warehouse engines to ingest raw S3 targets."""
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA
    )
    cursor = ctx.cursor()

    # Create explicit secure path point mapping inside Snowflake
    cursor.execute(f"""
        CREATE OR REPLACE STAGE my_s3_stage
        URL = 's3://{AWS_BUCKET_NAME}/'
        CREDENTIALS = (AWS_KEY_ID = '{AWS_ACCESS_KEY}' AWS_SECRET_KEY = '{AWS_SECRET_KEY}');
    """)

    # Bulk Copy structured datasets
    cursor.execute("""
        COPY INTO raw_transactions
        FROM @my_s3_stage/transactions/raw/
        FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """)

    # Bulk Copy unstructured variant datasets using a SELECT projection query format
    cursor.execute("""
        COPY INTO raw_unstructured_chats
        FROM (
            select $1:chat_id::varchar, current_timestamp(), $1
            from @my_s3_stage/chats/raw/
        )
        FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE);
    """)

    ctx.commit()
    cursor.close()
    ctx.close()
    print("✨ S3 Data Lake bulk load execution run completed successfully!")

with DAG(
    'cloud_retail_s3_data_lake_pipeline',
    default_args=default_args,
    description='Advanced S3 Staging & Data Lake Pipeline',
    schedule_interval=timedelta(minutes=10),
    catchup=False
) as dag:

    stage_to_s3 = PythonOperator(
        task_id='generate_and_stage_to_amazon_s3',
        python_callable=generate_and_upload_to_s3
    )

    load_to_snowflake = PythonOperator(
        task_id='bulk_load_s3_to_snowflake',
        python_callable=bulk_load_s3_to_snowflake
    )

    run_dbt_transformations = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='dbt run --project-dir /opt/airflow/dags/retail_transformation --profiles-dir /opt/airflow/dags/retail_transformation'
    )

    run_dbt_quality_tests = BashOperator(
        task_id='run_dbt_quality_tests',
        bash_command='dbt test --project-dir /opt/airflow/dags/retail_transformation --profiles-dir /opt/airflow/dags/retail_transformation'
    )

    # Linked step execution dependencies tree
    stage_to_s3 >> load_to_snowflake >> run_dbt_transformations >> run_dbt_quality_tests
