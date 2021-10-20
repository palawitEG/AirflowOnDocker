from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from elasticsearch_plugin.hooks.elastic_hook import ElasticHook

default_args = {
    "start_date": datetime(2021, 10, 20)
}

def _print_es_info():
    hook = ElasticHook()
    print(hook.info())

with DAG(
    "elastic_dag",
    default_args=default_args,
    description="elasticsearch",
    schedule_interval="@daily",
    tags=["myDAG"],
    catchup=False
) as dag:

    print_es_info = python_task = PythonOperator(
        task_id="print_es_info",
        python_callable=_print_es_info
    )