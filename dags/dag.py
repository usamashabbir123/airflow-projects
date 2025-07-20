from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from database.mysql import MySqlConnector
from processing.processing import DataClearing
class DatabaseConnectionSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    def poke(self, context):
        try:
            connector = MySqlConnector()
            connector.connect()
            return True
        except Exception as e:
            self.log.info(f"Database connection failed: {str(e)}")
            return False



def processed_data():
    connector = MySqlConnector()
    connector.connect()
    d=DataClearing(connector)
    d.load_data()
    d.save_data()
    connector.close()
    return "Data processed successfully"
default_args = {
    'owner': 'airflow', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
def currentuser():
    return 'usama shabbir'

# def printUser(**context):
#     ti = context['ti']
#     name=ti.xcom_pull(task_ids='task_1')
#     print(f"Hello, {name}")
with DAG(
    dag_id='data_clearning',
    start_date=datetime(2025, 7, 17),
    schedule_interval='* * * * *',
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    
    # Database connection sensor
    check_db_connection = DatabaseConnectionSensor(
        task_id='check_db_connection',
        poke_interval=60,  # Check every 60 seconds
        timeout=600,       # Timeout after 10 minutes
        mode='poke'       # Can be 'poke' or 'reschedule'
    )

    # Your other tasks
    task1 = PythonOperator(
        task_id='task_1',
        python_callable=processed_data

    )
    # Set dependencies
    check_db_connection >> task1
