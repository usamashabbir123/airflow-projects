from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os 
from connections.postgres.postgres import PostgresConnector 

from airflow.sensors.base import BaseSensorOperator
# Function to get database credentials from environment variables
def getDbCredentials():
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'airflow')
    password = os.getenv('POSTGRES_PASSWORD', 'airflow')    
    database = os.getenv('POSTGRES_DB', 'airflow')
    return host, port, user, password, database
# create a sensor to check if the database is ready
class DatabaseReadySensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    def poke(self, context):
        try:
            postgres = PostgresConnector(*getDbCredentials())
            postgres.connect()
            return True
        except Exception as e:
            self.log.error(f"Database not ready: {e}")
            return False
# Function to load data from airnum dataset and save it to the postgres database
def load_save_airbnb_data():
    postgres = PostgresConnector(*getDbCredentials())
    postgres.connect()
    print("Connected to PostgreSQL database successfully.")
    # postgres.load_data() 
    month = 5
    year = 2020
    i = 1
    while i  < 13:
        if month > 12:
            month = 1
            year += 1
        file_path = f'/opt/airflow/dags/datasets/airnub/{month:02d}_{year}.csv'
        print(f"Processing file for month {month}: {file_path}")
        if os.path.exists(file_path):
            print(f'inserting data airbnb for month {month} .......')
            postgres.load_data_from_Census_LGA(file_path, 'airbnb_listings_raw') 
            print(f"Data loaded successfully for month {month}.")
        else:
            print(f"File for month {month} does not exist: {file_path}")
        month += 1
        i += 1
    print("Data loaded successfully into G01 table.")
    postgres.close()
def load_save_censu_lga():
    postgres = PostgresConnector(*getDbCredentials())
    postgres.connect()
    print("Connected to PostgreSQL database successfully.")

    print('inserting data into G01 table .......')
    postgres.load_data_from_Census_LGA('/opt/airflow/dags/datasets/Census_LGA/2016Census_G01_NSW_LGA.csv', 'g01') 
    print("Data loaded successfully into G01 table.")

    print('inserting data into G02 table .......')
    postgres.load_data_from_Census_LGA('/opt/airflow/dags/datasets/Census_LGA/2016Census_G02_NSW_LGA.csv', 'g02') 
    print("Data loaded successfully into G02 table.")
    postgres.close()
def load_save_nsw_lga():
    postgres = PostgresConnector(*getDbCredentials())
    postgres.connect()
    print("Connected to PostgreSQL database successfully.")

    print('inserting data into nsw_lga_code table .......')
    postgres.load_data_from_Census_LGA('/opt/airflow/dags/datasets/NSW_LGA/NSW_LGA_CODE.csv', 'nsw_lga_code') 
    print("Data loaded successfully into nsw_lga_code table.")

    print('inserting data into lga_suburb table .......')
    postgres.load_data_from_Census_LGA('/opt/airflow/dags/datasets/NSW_LGA/NSW_LGA_SUBURB.csv', 'lga_suburb') 
    print("Data loaded successfully into lga_suburb table.")
    postgres.close()
default_args = {
    'owner': 'usama', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bronze_dag',
    start_date=datetime(2025, 7, 17),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    task1= DatabaseReadySensor(
        task_id='check_database_ready',
        poke_interval=10,  # Check every 10 seconds
        timeout=300,  # Timeout after 5 minutes
        mode='poke',  # Use poke mode
    )
    task2= PythonOperator(
        task_id='load_save_airbnb_data',
        python_callable=load_save_airbnb_data,
    )
    task3= PythonOperator(
        task_id='load_save_censu_lga',
        python_callable=load_save_censu_lga,
    )
    task4= PythonOperator(
        task_id='load_save_nsw_lga',
        python_callable=load_save_nsw_lga,
    )
    task5= BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/airbnb_dbt && dbt snapshot --profiles-dir /opt/airflow/.dbt',
    )
    task6= BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/airbnb_dbt && dbt run -f --profiles-dir /opt/airflow/.dbt',
    )
    task1 >>task2>>task3 >> task4 >>task6 >> task5
