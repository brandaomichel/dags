import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sdk import task

with DAG(
    dag_id='XcomDag',
    description='XcomDAG',
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz='America/Sao_Paulo'),
    catchup=False,
    tags=["curso", "xcom"]
) as dag:
    @task
    def task_write():
        return {"valorxcom1": 10000}
    
    @task
    def task_read(payload: dict):
        print(f'valor de retorno xcom: {payload['valorxcom1']}')
    
    task_read(task_write())