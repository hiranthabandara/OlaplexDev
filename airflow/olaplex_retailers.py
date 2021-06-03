import os
import logging
from airflow import DAG
from airflow.models import Variable
from airflow import AirflowException
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


retailers = Variable.get("retailer_config", deserialize_json=True)

HOME_DIR = os.path.expanduser('~')
SRC_HOME = os.path.join(HOME_DIR, 'dev_Sehan/Olaplex-Retail-Dev/retail/main')

def check_retailer_status(retailer_name):
    error_file = os.path.join(SRC_HOME, '_data', retailer_name, 'error.log')
    if os.path.isfile(error_file):
        with open(error_file) as f:
            logging.error(f.read())
        raise AirflowException(f"Errors found for the retailer {retailer_name}")
    else:
        logging.info(f"No error found for the retailer {retailer_name}")


default_arguments = {
    'owner': 'nabin',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 1),
    'email': ['test@test.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id="olapelx_retailers",
    default_args=default_arguments,
    catchup=False,
    schedule_interval="@daily"
)

def createRetailerTask(retailer_name, **args):
    task = BashOperator(
        task_id = retailer_name,
        bash_command=f"cd {SRC_HOME} && python3 main.py extract_retailer_data {retailer_name}" ,
        dag=dag
    )
    return task


def createCheckStatusTask(retailer_name, **args):
    task = PythonOperator(
        task_id = f"check_{retailer_name}_status",
        python_callable=check_retailer_status,
        op_kwargs={'retailer_name': retailer_name},
        dag=dag
    )
    return task

start = BashOperator(
    dag=dag,
    task_id='start',
    bash_command=f"echo hello"
)

sync_git_repos = BashOperator(
    dag=dag,
    task_id='sync_git_repos',
    bash_command=f"cd {SRC_HOME} && git pull origin Sehan_dev"
)
start >> sync_git_repos

load_to_stg = BashOperator(
    dag=dag,
    task_id='LoadToStagingTables',
    bash_command=f"cd {SRC_HOME} && python3 main.py load_to_staging_tables",
    trigger_rule='none_skipped'
    )

for k,v in retailers.items():
    if v["enable"] == "yes":
        retailer_task = createRetailerTask(retailer_name=k)
        check_status = createCheckStatusTask(retailer_name=k)
        sync_git_repos >> retailer_task >> check_status >> load_to_stg


load_to_dwh = BashOperator(
    dag=dag,
    task_id='LoadToDWH',
    bash_command=f"cd {SRC_HOME} && python3 main.py load_to_final_table"
)

load_to_stg >> load_to_dwh