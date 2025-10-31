import pendulum
from airflow import DAG
from airflow.decorators import task

ITENS = ["sp", "rj"," mg", "rs"]

with DAG(
    dag_id="Dinamicas",
    description="Dinamicas",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:
    
    @task
    def baixar(nome: str) -> str:
        print(f"Baixando {nome}..")
        return nome

    @task
    def processar(nome: str) -> str:
        print(f"Processando {nome}...")
        return f"OK {nome}"

    @task
    def consolidar(resultados: list[str]) -> str:
        print("Consolidados: ", resultados)
    
    baixados = baixar.expand(nome=ITENS)
    processados = processar.expand(nome=baixados)
    consolidar(processados)