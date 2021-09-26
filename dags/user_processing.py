from airflow.models import DAG
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