import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator

default_args = {
    "depends_on_past": False,
    "email": ["teste@gmails.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}
with DAG(
    dag_id='default_args_dag',
    description='Default_Args Dag',
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz='America/Sao_Paulo'),
    catchup=False,
    tags=["args", "exemplo"]
) as dag:
    task1 = BashOperator(task_id='tsk1', bash_command='sleep 5', retries=3)
    task2 = BashOperator(task_id='tsk2', bash_command='sleep 5')
    task3 = BashOperator(task_id='tsk3', bash_command='sleep 5')

    task1 >> task2 >> task3
