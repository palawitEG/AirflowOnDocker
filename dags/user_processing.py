from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pandas import json_normalize
import json

default_args = {
    "start_date": datetime(2021, 1, 10)
}

def _processing_user(ti):
    users = ti.xcom_pull(task_ids=["extracting_user"])
    if not len(users) or "results" not in users[0]:
        raise ValueError("User is empty.")
    user = users[0]["results"][0]
    processed_user = json_normalize({
        "username": user["login"]["username"],
        "password": user["login"]["password"],
        "fname": user["name"]["first"],
        "lname": user["name"]["last"]
    })
    processed_user.to_csv("/opt/airflow/dbs/processing_user.csv", index=None, header=False)
    

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

    extracting_user = SimpleHttpOperator(
        task_id="extracting_user",
        http_conn_id="user_api",
        method="GET",
        endpoint="api/",
        response_filter= lambda response: json.loads(response.text),
        log_response=True
    )

    python_task = PythonOperator(
        task_id="processing_user",
        python_callable=_processing_user
    )
     