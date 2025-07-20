from airflow.decorators import dag, task
from datetime import datetime, timedelta  
default_args = {
    'owner': 'airflow', 
    'start_date': datetime(2025, 7, 19),
}


@dag(
    dag_id='api_dag',
    default_args=default_args,
    schedule_interval='* * * * *',
    catchup=False,
    )
def user_workflow():
    @task
    def getUser():
        return 'usama shabbir'
    @task
    def getAge():
        return 25
    @task
    def printUser(name, age):
        print(f"Hello, {name}, you are {age} years old.")

    username= getUser()
    userage= getAge()
    printUser(name=username, age=userage)   

dag_instance = user_workflow()