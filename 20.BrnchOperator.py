import pendulum
import random
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import get_current_context

with DAG(
    dag_id='DagBranchOperator',
    description='DagBranchOperator',
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz='America/Sao_Paulo'),
    catchup=False,
    tags=["curso", "exemplo", "Pools"]
) as dag:
    def gerar_num():
        return random.randint(1,100)
    
    gera_num_task = PythonOperator(
        task_id='gera_num_task',
        python_callable=gerar_num
    )

    def avalia_par_impar():
        ctx = get_current_context()
        numero = ctx['ti'].xcom_pull(task_ids='gera_num_task')
        return 'par_task' if numero % 2 == 0 else 'impar_task'
    
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=avalia_par_impar
    )

    par_task = BashOperator(task_id='par_task', bash_command='echo "Numero par"')
    impar_task = BashOperator(task_id='impar_task', bash_command='echo "Numero impar"')

    gera_num_task >> branch_task >> [par_task, impar_task]