from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime

default_args = {
    "start_date": datetime(2021, 1, 10)
}

with DAG(
    "user_processing",
    default_args=default_args,
    description="A simple tutorial DAG",
    tags=["example"],
    catchup=False
) as dag:
    # task do here!
    creating_table = SqliteOperator(
        task_id="creating_table",
        sqlite_conn_id="db_sqlite",
        sql = '''
            CREATE TABLE users(
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                fname TEXT NOT NULL,
                lname TEXT NOT NULL
            );
            '''
    )

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="user_api",
        endpoint="api/"
    )