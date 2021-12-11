from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from health.etl.extract import ExtractData

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["tsoares.bruno@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# TODO: desenvolver os jobs que serão chamados pelas tasks

e = ExtractData()


def job_extract_percepcao_saude():
    URL = "https://ftp.ibge.gov.br/PNS/2019/"
    PARAM = "Divulgacoes/Volume_4/tabelas_xls/"
    INDICES_TBLS_PNS = ["N", "P", "Q", "U"]

    for index in INDICES_TBLS_PNS:
        PATH = "../../datalake/bronze/percepcao_saude/"
        e.extract_zipfile(url_zipfile=f"{URL}{PARAM}Modulo_{index}.zip", dir=PATH)


def job_extract_ciclos_de_vida():

    URL = "https://ftp.ibge.gov.br/PNS/2019/Divulgacoes"
    PARAM = "/Volume_6/tabelas_xls/Modulo_K.zip"
    PATH = "../../datalake/bronze/ciclos_vida/"

    e.extract_zipfile(url_zipfile=f"{URL}{PARAM}", dir=PATH)


def job_extract_deficit_peso():
    URI_ROOT = "https://sidra.ibge.gov.br/geratabela?"
    URI_PARAM = "format=br.csv&name=tabela8169.csv&terr=N&rank=-&"
    QUERY_A = "query=t/8169/n1/all/v/11643/p/all/c2/6794/c58"
    QUERY_B = "/95253/d/v11643%200/l/v,p%2Bc2,t%2Bc58"
    PATH = "../../datalake/bronze/deficit_peso/"

    e.extract_csvfile(
        read_csv_file=f"{URI_ROOT}{URI_PARAM}{QUERY_A}{QUERY_B}", path_name_file=PATH
    )


with DAG(
    "Extract Data",
    default_args=default_args,
    description="A job for extract the data in something websites",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 12, 13),
    catchup=False,
    tags=["example"],
) as dag:

    t1 = PythonOperator(
        task_id="job extract percepção de saúde",
        python_callable=job_extract_percepcao_saude,
    )

    t2 = PythonOperator(
        task_id="job extract ciclos de vida", python_callable=job_extract_ciclos_de_vida
    )

    t3 = PythonOperator(
        task_id="job extract deficit de peso", python_callable=job_extract_deficit_peso
    )

    [t1, t2, t3]
