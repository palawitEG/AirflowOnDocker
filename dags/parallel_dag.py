from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subDAGs.subdag_parallel_dag import subdag_parallel_dag

default_args = {
    "start_date": datetime(2021, 10, 3)
}

with DAG(
    "parallel_dag",
    default_args=default_args,
    description="A simple parallel DAG",
    schedule_interval="@daily",
    tags=["example"],
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id="task-1",
        bash_command="sleep 3 && echo 'task-1 done.'"
    )

    processing = SubDagOperator(
        task_id="processing_tasks",
        subdag=subdag_parallel_dag("parallel_dag", "processing_tasks", default_args)
    )

    task4 = BashOperator(
        task_id="task-4",
        bash_command="sleep 3 && echo 'task-4 done.'"
    )

    task1 >> processing >> task4