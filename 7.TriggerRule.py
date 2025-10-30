import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='trigger_rule_dag3',
    description='Trigger Rule Dag3',
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz='America/Sao_Paulo'),
    catchup=False,
    tags=["args", "exemplo"]
) as dag:
    task1 = BashOperator(task_id='tsk1', bash_command='exit 1')
    task2 = BashOperator(task_id='tsk2', bash_command='exit 1')
    task3 = BashOperator(task_id='tsk3', bash_command='sleep 5', trigger_rule=TriggerRule.ALL_FAILED)

    [task1 ,task2] >> task3
