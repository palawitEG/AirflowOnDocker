from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

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

    task2 = BashOperator(
        task_id="task-2",
        bash_command="sleep 3 && echo 'task-2 done.'"
    )

    task3 = BashOperator(
        task_id="task-3",
        bash_command="sleep 3 && echo 'task-3 done.'"
    )

    task4 = BashOperator(
        task_id="task-4",
        bash_command="sleep 3 && echo 'task-4 done.'"
    )

    task1 >> [task2, task3] >> task4