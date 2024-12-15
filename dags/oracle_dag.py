from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from datetime import datetime
 
 
def consulta_oracle():
    oracle_hook = OracleHook(oracle_conn_id="oracle_conn")
    sql = "SELECT * FROM SEG_ACCESO"
    result = oracle_hook.get_records(sql)
    for row in result:
        print(row)
 
 
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 10),
}
 
dag = DAG(
    "oracle_connection_example",
    default_args=default_args,
    schedule_interval=None,
)
 
t1 = PythonOperator(
    task_id="consulta_oracle",
    python_callable=consulta_oracle,
    dag=dag,
)
 