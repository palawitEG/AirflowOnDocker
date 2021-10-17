from airflow import DAG
from airflow.operators.bash import BashOperator

def subdag_parallel_dag(parent_dag_id, child_dag_id, default_args):
    with DAG(dag_id=f"{parent_dag_id}.{child_dag_id}",default_args=default_args) as dag:
        
        task2 = BashOperator(
        task_id="task-2",
        bash_command="sleep 3 && echo 'task-2 done.'"
        )

        task3 = BashOperator(
            task_id="task-3",
            bash_command="sleep 3 && echo 'task-3 done.'"
        )

        return dag