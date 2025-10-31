import pendulum
import random
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import get_current_context

with DAG(
    dag_id='DagShortCircuit',
    description='DagShortCircuit',
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz='America/Sao_Paulo'),
    catchup=False,
    tags=["curso", "exemplo", "Pools"]
) as dag:
    def gerar_qualidade() -> int:
        return random.randint(1,100)
    
    gera_qualidade_task = PythonOperator(
        task_id='gera_qualidade_task',
        python_callable=gerar_qualidade,
    )

    def avalia_qualiade() -> bool:
        ctx = get_current_context()['ti'].xcom_pull(task_ids='gera_qualidade_task')
        qualidade = ctx
        return int(qualidade) >= 70
    
    short_circuit = ShortCircuitOperator(
        task_id='short_circuit',
        python_callable=avalia_qualiade,
    )

    processa = BashOperator(task_id='processa', bash_command='echo "Processado por qualidade Boa"')
    finaliza = BashOperator(task_id='finaliza', bash_command='echo "Finalizado por qualidade boa"')

    gera_qualidade_task >> short_circuit >> processa >> finaliza