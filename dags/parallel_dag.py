from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
# from subDAGs.subdag_parallel_dag import subdag_parallel_dag
from airflow.utils.task_group import TaskGroup

default_args = {
    "start_date": datetime(2021, 10, 3)
}

with DAG(
    "parallel_dag",
    default_args=default_args,
    description="A simple parallel DAG",
    schedule_interval="@daily",
    tags=["myDAG"],
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id="task-1",
        bash_command="sleep 3 && echo 'task-1 done.'"
    )

    with TaskGroup("processing_tasks") as processing_tasks:
        
        task2 = BashOperator(
            task_id="task-2",
            bash_command="sleep 3 && echo 'task-2 done.'"
        )

        with TaskGroup("spark_processing_group") as spark_processing_group:
            task3 = BashOperator(
            task_id="task-3",
            bash_command="sleep 3 && echo 'task-3 done.'"
        )

        with TaskGroup("flink_processing_group") as flink_processing_group:
            task3 = BashOperator(
            task_id="task-3",
            bash_command="sleep 3 && echo 'task-3 done.'"
        )

    # processing = SubDagOperator(
    #     task_id="processing_tasks",
    #     subdag=subdag_parallel_dag("parallel_dag", "processing_tasks", default_args)
    # )

    task4 = BashOperator(
        task_id="task-4",
        bash_command="sleep 3 && echo 'task-4 done.'"
    )

    task1 >> processing_tasks >> task4